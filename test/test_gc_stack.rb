# frozen_string_literal: true

# Regression test for the per-thread GC stack-bottom overrides in
# templates/index.cr.
#
# The template overrides GC.current_thread_stack_bottom and
# GC.set_stackbottom with compile-time dispatch:
#
#   If the stdlib provides LibGC.set_stackbottom / get_my_stackbottom
#   (via -Dpreview_mt, win32, or BDW-GC >= 8.2.0), the thread-local BDW-GC
#   API is used.  Otherwise we fall back to the global GC_stackbottom
#   variable (the "legacy" path in the stdlib's own words).
#
# These methods are called by Crystal's Fiber and Scheduler internals.
# A broken override (e.g. "fun redefinition with different signature")
# prevents compilation entirely, while a broken implementation causes
# segfaults or missing GC roots in multi-threaded scenarios.
#
# This test verifies that whichever branch was compiled for the current
# platform produces working code — the GC stack-bottom methods can be
# called without crashing and return consistent results.

require_relative "test_helper"

class TestGCStack < Minitest::Test
  module GcStackTest
    # GC.current_thread_stack_bottom returns a tuple
    # {Pointer(Void), Pointer(Void)}.  The second element (mem_base /
    # GC_stackbottom) may be null on older BDW-GC without thread-local
    # stack registration (the "legacy" path).  We cannot assert non-null
    # portably, but we CAN assert that repeated calls return the same
    # value — confirming the override compiles and runs without crashing.
    crystallize -> { Bool }
    def stack_bottom_consistent?
      b1 = GC.current_thread_stack_bottom
      b2 = GC.current_thread_stack_bottom
      b1[1] == b2[1]
    end

    # GC.set_stackbottom associates a stack base with the current thread.
    # Even passing a null pointer should not crash — it's a no-op update.
    crystallize -> { Bool }
    def set_stackbottom_ok?
      GC.set_stackbottom(Pointer(Void).null)
      true
    end
  end

  def test_current_thread_stack_bottom_is_consistent
    assert GcStackTest.stack_bottom_consistent?,
      "GC.current_thread_stack_bottom returned inconsistent values"
  end

  def test_set_stackbottom_does_not_crash
    assert GcStackTest.set_stackbottom_ok?,
      "GC.set_stackbottom raised or returned false"
  end
end
