require "json"

module CrystalRuby
  # The Reactor represents a singleton Thread responsible for running all
  # Ruby/Crystal interop code.  Crystal's Fiber scheduler and GC assume
  # all code is run on a single thread.  This class multiplexes Ruby and
  # Crystal calls onto a single reactor thread (the default) OR, for
  # synchronous (blocking + non-async) Crystal methods, allows direct
  # FFI invocation from any Ruby thread with proper BDW-GC registration.
  #
  # Fast path (bypasses the queue):
  #   Synchronous Crystal calls from foreign threads register the calling
  #   thread with BDW-GC (GC_get_stack_base + GC_register_my_thread +
  #   Thread.current) once, then call FFI directly.  This is safe because:
  #
  #   1. BDW-GC has allow_register_threads enabled, turning on its
  #      internal locking (GC_need_to_lock) for multi-threaded access.
  #   2. Crystal's Thread.current lazily creates per-thread state and
  #      uses a thread-safe linked list (Crystal >= 1.16).
  #   3. Synchronous Crystal methods do not touch the fiber scheduler
  #      or reactor thread-local state.
  #
  # Async methods and blocking methods that yield to the scheduler
  # still go through the reactor queue as before.
  module Reactor
    module_function

    class SingleThreadViolation < StandardError; end

    class StopReactor < StandardError; end

    @op_count = 0
    @single_thread_mode = false

    REACTOR_QUEUE = Queue.new

    # Invoke GC every 100 ops
    GC_OP_THRESHOLD = ENV.fetch("CRYSTAL_GC_OP_THRESHOLD", 100).to_i
    # Or every 0.05 seconds
    GC_INTERVAL = ENV.fetch("CRYSTAL_GC_INTERVAL", 0.05).to_f
    # Or if we've gotten hold of a reference to at least 100KB or more of fresh memory since last GC
    GC_BYTES_SEEN_THRESHOLD = ENV.fetch("CRYSTAL_GC_BYTES_SEEN_THRESHOLD", 100 * 1024).to_i

    # Reduce expensive gc_due? checks (clock_gettime) — only perform the
    # full check every N operations instead of every single one.
    GC_CHECK_INTERVAL = ENV.fetch("CRYSTAL_GC_CHECK_INTERVAL", 10).to_i

    # We maintain a map of threads, each with a mutex, condition variable, and result
    THREAD_MAP = Hash.new do |h, tid_or_thread, tid = tid_or_thread|
      if tid_or_thread.is_a?(Thread)
        ObjectSpace.define_finalizer(tid_or_thread) do
          THREAD_MAP.delete(tid_or_thread)
          THREAD_MAP.delete(tid_or_thread.object_id)
        end
        tid = tid_or_thread.object_id
      end

      h[tid] = {
        mux: Mutex.new,
        cond: ConditionVariable.new,
        result: nil,
        thread_id: tid
      }
      h[tid_or_thread] = h[tid] if tid_or_thread.is_a?(Thread)
    end

    # We memoize callbacks, once per return type
    CALLBACKS_MAP = Hash.new do |h, rt|
      h[rt] = FFI::Function.new(:void, [:int, *((rt == :void) ? [] : [rt])]) do |tid, ret|
        THREAD_MAP[tid][:error] = nil
        THREAD_MAP[tid][:result] = ret
        THREAD_MAP[tid][:cond].signal
      end
    end

    ERROR_CALLBACK = FFI::Function.new(:void, %i[string string string int]) do |error_type, message, backtrace, tid|
      error_type = error_type.to_sym
      is_exception_type = Object.const_defined?(error_type) && Object.const_get(error_type).ancestors.include?(Exception)
      error_type = is_exception_type ? Object.const_get(error_type) : RuntimeError
      error = error_type.new(message)
      error.set_backtrace(JSON.parse(backtrace))
      raise error unless THREAD_MAP.key?(tid)

      THREAD_MAP[tid][:error] = error
      THREAD_MAP[tid][:result] = nil
      THREAD_MAP[tid][:cond].signal
    end

    def thread_conditions
      THREAD_MAP[Thread.current]
    end

    def await_result!
      mux, cond, result, err = thread_conditions.values_at(:mux, :cond, :result, :error)
      cond.wait(mux) unless result || err
      result, err, thread_conditions[:result], thread_conditions[:error] = thread_conditions.values_at(:result, :error)
      if err
        combined_backtrace = err.backtrace[0..(err.backtrace.index { |m|
                                                 m.include?("call_blocking_function")
                                               } || 2) - 3] + caller[5..-1]
        err.set_backtrace(combined_backtrace)
        raise err
      end

      result
    end

    def halt_loop!
      raise StopReactor
    end

    def stop!
      return unless @main_loop

      schedule_work!(self, :halt_loop!, :void, blocking: true, async: false)
      @main_loop.join
      @main_loop = nil
      CrystalRuby.log_info "Reactor loop stopped"
    end

    def start!
      @op_count = 0
      @main_loop ||= Thread.new do
        @main_thread_id = Thread.current.object_id
        CrystalRuby.log_debug("Starting reactor")
        CrystalRuby.log_debug("CrystalRuby initialized")
        while true
          handler, *args, lib = REACTOR_QUEUE.pop
          send(handler, *args, lib)
          @op_count += 1
          invoke_gc_if_due!(lib)
        end
      rescue StopReactor
      rescue => e
        CrystalRuby.log_error "Error: #{e}"
        CrystalRuby.log_error e.backtrace
      end
    end

    def invoke_gc_if_due!(lib)
      return unless lib

      return unless gc_due?

      ensure_thread_registered!(lib)
      lib.gc
    end

    def ensure_thread_registered!(lib)
      return if Thread.current[:cr_registered] || @single_thread_mode || Thread.current.object_id == @main_thread_id

      lib.register_thread
      Thread.current[:cr_registered] = true
    end

    def gc_due?
      # Cheap: memory-bytes-seen is a simple counter read, not a syscall.
      # Check it on every call so large allocations are never missed.
      if Types::Allocator.gc_bytes_seen > GC_BYTES_SEEN_THRESHOLD
        Types::Allocator.gc_hint_reset!
        return true
      end

      # Expensive: clock_gettime and arithmetic — only every Nth call.
      @gc_check_count ||= 0
      @gc_check_count += 1
      return false unless (@gc_check_count % GC_CHECK_INTERVAL) == 0

      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      @last_gc_time ||= now
      @op_count ||= 0
      @last_gc_op_count ||= @op_count

      ops_since_last_gc = @op_count - @last_gc_op_count
      time_since_last_gc = now - @last_gc_time

      if ops_since_last_gc >= GC_OP_THRESHOLD || time_since_last_gc >= GC_INTERVAL
        @last_gc_time = now
        @last_gc_op_count = @op_count
        true
      else
        false
      end
    end

    def thread_id
      Thread.current.object_id
    end

    def yield!(lib: nil, time: 0.0)
      schedule_work!(lib, :yield, :int, async: false, blocking: false, lib: lib) if running? && lib
      nil
    end

    def invoke_async!(receiver, op_name, *args, thread_id, callback, lib)
      receiver.send(op_name, *args, thread_id, callback)
      yield!(lib: lib, time: 0)
    end

    def invoke_blocking!(receiver, op_name, *args, tvars, _lib)
      tvars[:error] = nil
      begin
        tvars[:result] = receiver.send(op_name, *args)
      rescue StopReactor
        tvars[:cond].signal
        raise
      rescue => e
        tvars[:error] = e
      end
      tvars[:cond].signal
    end

    def invoke_await!(receiver, op_name, *args, lib)
      outstanding_jobs = receiver.send(op_name, *args)
      yield!(lib: lib, time: 0) unless outstanding_jobs == 0
    end

    # Schedule Crystal work onto the reactor thread (or take the fast
    # path for synchronous calls from foreign threads).
    #
    # Thread safety for the fast path:
    # --------------------------------
    # BDW-GC has been initialised with `allow_register_threads` (see
    # index.cr), which enables GC_need_to_lock — the GC's internal
    # mutex — making GC_{malloc,free,collect} thread-safe.
    #
    # Each foreign thread registers itself with BDW-GC once by calling
    # the library's `register_thread` FFI function, which runs on that
    # thread and calls:
    #   GC_get_stack_base     — thread-safe BDW-GC call
    #   GC_register_my_thread — thread-safe BDW-GC call
    #   Thread.current        — creates per-thread state via Crystal's
    #                           thread-safe linked list (Crystal >= 1.16)
    #
    # After registration the thread may call synchronous Crystal FFI
    # functions directly.  These functions do not invoke the fiber
    # scheduler or touch reactor-thread-local state, so no queue
    # roundtrip is needed.
    def schedule_work!(receiver, op_name, *args, return_type, blocking: true, async: true, lib: nil)
      # Fast path 1: already on the reactor / single-thread mode
      if @single_thread_mode || (Thread.current.object_id == @main_thread_id && op_name != :yield)
        unless Thread.current.object_id == @main_thread_id
          raise SingleThreadViolation,
            "Single thread mode is enabled, cannot run in multi-threaded mode. " \
            "Reactor was started from: #{@main_thread_id}, then called from #{Thread.current.object_id}"
        end
        invoke_gc_if_due!(lib)
        return receiver.send(op_name, *args)
      end

      # Fast path 2: synchronous Crystal call from a foreign thread.
      # Register the thread with BDW-GC on first use, then call FFI
      # directly instead of routing through the reactor queue.
      if blocking && !async && lib
        return invoke_sync_direct!(receiver, op_name, *args, lib: lib)
      end

      tvars = thread_conditions
      tvars[:mux].synchronize do
        REACTOR_QUEUE.push(
          case true
          when async then [:invoke_async!, receiver, op_name, *args, tvars[:thread_id], CALLBACKS_MAP[return_type], lib]
          when blocking then [:invoke_blocking!, receiver, op_name, *args, tvars, lib]
          else [:invoke_await!, receiver, op_name, *args, lib]
          end
        )
        return await_result! if blocking
      end
    end

    def invoke_sync_direct!(receiver, op_name, *args, lib:)
      ensure_thread_registered!(lib)
      receiver.send(op_name, *args)
    end

    def running?
      @main_loop&.alive?
    end

    def init_single_thread_mode!
      @single_thread_mode ||= begin
        @main_thread_id = Thread.current.object_id
        true
      end
    end
  end
end
