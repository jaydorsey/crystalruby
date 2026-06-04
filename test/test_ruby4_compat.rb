# frozen_string_literal: true

# Regression tests for Ruby 4.0 / nt_start compatibility.
#
# Root cause: In Ruby 4.0 the native-thread launch path changed to nt_start,
# which does not properly prime Crystal's __thread TLS before Thread::current
# is first accessed during Crystal::once inside main_user_code.  The fix moves
# the Crystal `init` FFI call onto the attaching (main) thread, and then
# schedules a `register_thread` call on the reactor thread so it receives a
# valid Thread::current context (Crystal >= 1.16.0) before any user code runs.

require_relative "test_helper"

class TestRuby4Compat < Minitest::Test
  # A dedicated library used only by these tests so compilation is isolated.
  module Ruby4TestLib
    crystallize :int32
    def add_ints(a: :int32, b: :int32)
      a + b
    end

    crystallize :string
    def echo_string(s: :string)
      s
    end
  end

  # ── basic functionality ────────────────────────────────────────────────────

  # Verify that a Crystal function works correctly after the library has been
  # attached (init ran on the main thread, register_thread ran on the reactor).
  def test_crystal_function_works_after_attach
    assert_equal 7, Ruby4TestLib.add_ints(3, 4)
  end

  def test_crystal_string_roundtrip
    assert_equal "hello", Ruby4TestLib.echo_string("hello")
  end

  # ── concurrent access ─────────────────────────────────────────────────────

  # Simulate the Puma multi-threaded scenario: multiple Ruby threads calling
  # Crystal functions concurrently.  Before the fix this would segfault on
  # Ruby 4.0 because init was attempted on a reactor thread whose TLS had
  # never been set up by Crystal.
  def test_concurrent_calls_from_multiple_threads
    return if CrystalRuby.config.single_thread_mode

    results = Array.new(10)
    threads = 10.times.map do |i|
      Thread.new { results[i] = Ruby4TestLib.add_ints(i, i) }
    end
    threads.each(&:join)

    10.times { |i| assert_equal i * 2, results[i] }
  end

  # Verify that calling the same Crystal function repeatedly from many threads
  # doesn't corrupt shared state or crash.
  def test_high_concurrency_string_echo
    return if CrystalRuby.config.single_thread_mode

    errors  = []
    threads = 20.times.map do |i|
      Thread.new do
        result = Ruby4TestLib.echo_string("thread_#{i}")
        errors << "mismatch at #{i}" unless result == "thread_#{i}"
      end
    end
    threads.each(&:join)

    assert_empty errors, "Concurrent string echo produced: #{errors.join(", ")}"
  end

  # ── init idempotency ──────────────────────────────────────────────────────

  # Crystal's init function is guarded by an `initialized` flag.  Verifies
  # that the library still works when accessed after having already been
  # attached (guards against double-init regressions).
  def test_library_usable_after_repeated_access
    3.times { assert_equal 5, Ruby4TestLib.add_ints(2, 3) }
  end

  # ── reactor thread registration ───────────────────────────────────────────

  # Verify that register_thread being scheduled on the reactor thread does not
  # raise or stall.  If it did, the blocking schedule_work! call in attach!
  # would never return and the test would time out rather than hang silently.
  def test_register_thread_does_not_stall
    return if CrystalRuby.config.single_thread_mode

    # A successful call to any Crystal function implies register_thread
    # completed without blocking — otherwise attach! would have deadlocked.
    assert_equal 0, Ruby4TestLib.add_ints(0, 0)
  end
end
