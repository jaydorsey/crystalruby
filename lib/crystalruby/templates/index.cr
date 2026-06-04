module CrystalRuby
  ARGV1 = "crystalruby"

  alias ErrorCallback = (Pointer(::UInt8), Pointer(::UInt8), Pointer(::UInt8), ::UInt32 -> Void)

  class_property libname : String = "crystalruby"
  class_property callbacks : Channel(Proc(Nil)) = Channel(Proc(Nil)).new
  class_property rc_mux : Pointer(Void) = Pointer(Void).null
  class_property task_counter : Atomic(Int32) = Atomic(Int32).new(0)

  # Initializing Crystal Ruby invokes init on the Crystal garbage collector.
  # We need to be sure to only do this once.
  class_property initialized : Bool = false

  # We can override the error callback to catch errors in Crystal,
  # and explicitly expose them to Ruby.
  @@error_callback : ErrorCallback?

  # This is the entry point for instantiating CrystalRuby
  # We:
  # 1. Initialize the Crystal garbage collector
  # 2. Set the error callback
  # 3. Call the Crystal main function
  def self.init(libname : Pointer(::UInt8), @@error_callback : ErrorCallback, @@rc_mux : Pointer(Void))
    return if self.initialized
    self.initialized = true
    argv_ptr = ARGV1.to_unsafe
    {%% if compare_versions(Crystal::VERSION, "1.16.0") >= 0  %%}
      Crystal.init_runtime
    {%% end %%}
    Crystal.main_user_code(0, pointerof(argv_ptr))
    self.libname = String.new(libname)
    GC.init
    # Must be called after GC_init and before any GC_register_my_thread calls.
    # This enables GC_need_to_lock so foreign threads can register themselves.
    LibGCPrivate.allow_register_threads
  end

  # Explicit error handling (triggers exception within Ruby on the same thread)
  def self.report_error(error_type : String, message : String, backtrace : String, thread_id : UInt32)
    if error_reporter = @@error_callback
      error_reporter.call(error_type.to_unsafe, message.to_unsafe, backtrace.to_unsafe, thread_id)
    end
  end

  # New async task started
  def self.increment_task_counter
    @@task_counter.add(1)
  end

  # Async task finished
  def self.decrement_task_counter
    @@task_counter.sub(1)
  end

  # Get number of outstanding tasks
  def self.get_task_counter : Int32
    @@task_counter.get
  end

  # Queue a callback for an async task
  def self.queue_callback(callback : Proc(Nil))
    self.callbacks.send(callback)
  end

  def self.synchronize(&)
    LibC.pthread_mutex_lock(self.rc_mux)
    yield
    LibC.pthread_mutex_unlock(self.rc_mux)
  end
end

# Initialize CrystalRuby
fun init(libname : Pointer(::UInt8), cb : CrystalRuby::ErrorCallback, rc_mux : Pointer(Void)) : Void
  CrystalRuby.init(libname, cb, rc_mux)
end

fun stop : Void
  LibGC.deinit
end

# Register the calling thread with Crystal's runtime.
#
# Called from the reactor thread immediately after it starts (via Ruby's
# Reactor.schedule_work!) so that the reactor thread has a valid
# Thread::current and can safely use Crystal's fiber scheduler.
#
# IMPORTANT: We do NOT call Crystal.init_runtime here.  That function is
# meant for process-wide initialization (it resets @@threads, @@fibers, and
# Crystal::Once state).  Calling it on the reactor thread would destroy the
# main thread's Thread registration in @@threads, making Thread objects
# invisible to GC (which does not scan TLS) and eventually causing SIGSEGV
# when TLS returns a collected Thread pointer.
#
# Instead we just access Thread.current, which lazily creates a per-thread
# Thread object (with its main fiber and scheduler) without touching global
# bookkeeping that another thread depends on.
fun register_thread() : Void
  sb = LibGC::StackBase.new
  LibGCPrivate.get_stack_base(pointerof(sb))
  LibGCPrivate.register_my_thread(pointerof(sb))
  # Ensure a Thread object exists for the reactor thread.
  Thread.current
end

# Crystal's stdlib (gc/boehm.cr) already declares @[Link("gc")], so
# annotating our lib block too would produce duplicate -lgc linker flags.
#
# The stdlib conditionally declares `set_stackbottom` / `get_my_stackbottom`
# in LibGC (requires -Dpreview_mt, win32, or BDW-GC >= 8.2.0).  We use
# compile-time reflection (`LibGC.has_method?`) below to dispatch to the
# right API, so we never declare GC_set_stackbottom ourselves — avoiding
# "fun redefinition with different signature" on newer Crystal versions.
lib LibGC
  fun deinit = GC_deinit
  fun set_finalize_on_demand = GC_set_finalize_on_demand(Int32)
  fun invoke_finalizers = GC_invoke_finalizers : Int
end

# Non-standard BDW-GC functions (get_stack_base, register_my_thread,
# allow_register_threads) that Crystal's stdlib never declares.
lib LibGCPrivate
  fun get_stack_base = GC_get_stack_base(sb : LibGC::StackBase*) : Int32
  fun register_my_thread = GC_register_my_thread(sb : LibGC::StackBase*) : Int32
  fun allow_register_threads = GC_allow_register_threads
end

lib LibC
  fun calloc = calloc(Int32, Int32) : Void*
end

module GC
  def self.current_thread_stack_bottom
    {%% if LibGC.has_method?(:get_my_stackbottom) %%}
      sb = LibGC::StackBase.new
      LibGC.get_my_stackbottom(pointerof(sb))
      {Pointer(Void).null, sb.mem_base}
    {%% else %%}
      {Pointer(Void).null, LibGC.stackbottom}
    {%% end %%}
  end

  def self.set_stackbottom(stack_bottom : Void*)
    {%% if LibGC.has_method?(:set_stackbottom) %%}
      sb = LibGC::StackBase.new
      sb.mem_base = stack_bottom
      LibGC.set_stackbottom(nil, pointerof(sb))
    {%% else %%}
      LibGC.stackbottom = stack_bottom
    {%% end %%}
  end

  def self.collect
    LibGC.collect
    LibGC.invoke_finalizers
  end
end

# Trigger GC
fun gc : Void
  GC.collect
end

# Yield to the Crystal scheduler from Ruby
# If there's callbacks to process, we flush them
# Otherwise, we yield to the Crystal scheduler and let Ruby know
# how many outstanding tasks still remain (it will stop yielding to Crystal
# once this figure reaches 0).
fun yield : Int32
  Fiber.yield
  loop do
    select
    when callback = CrystalRuby.callbacks.receive
      callback.call
    else
      break
    end
  end
  CrystalRuby.get_task_counter
end

class Array(T)
  def initialize(size : Int32, @buffer : Pointer(T))
    @size = size.to_i32
    @capacity = @size
  end
end

require "json"
%{requires}
