# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import asyncio
import gc
import os
import threading


def find_future_source_sync():
    """Synchronous function to find and analyze futures with many callbacks."""
    print("\n--- Analyzing futures with many callbacks ---")

    # Find all tasks and their waiters
    tasks_with_waiters = []
    for task in asyncio.all_tasks():
        try:
            fut_waiter = getattr(task, '_fut_waiter', None)
            if fut_waiter and hasattr(fut_waiter, '_callbacks') and len(fut_waiter._callbacks) > 50:
                tasks_with_waiters.append((task, fut_waiter))
        except Exception:
            pass

    if not tasks_with_waiters:
        print("No futures with many callbacks found")
        return

    # Group by future to see if many tasks are waiting on the same future
    futures_map = {}
    for task, fut in tasks_with_waiters:
        futures_map.setdefault(id(fut), []).append(task)

    # Analyze each problematic future
    for fut_id, waiting_tasks in futures_map.items():
        fut = waiting_tasks[0]._fut_waiter  # Get the future from the first task
        print(f"\nFuture {fut_id} has {len(waiting_tasks)} tasks waiting on it")

        # Look at the callbacks to determine what's going on
        if hasattr(fut, '_callbacks'):
            callback_types = {}
            for cb in fut._callbacks:
                if isinstance(cb, tuple) and cb and callable(cb[0]):
                    cb_name = cb[0].__qualname__ if hasattr(cb[0], '__qualname__') else str(cb[0])
                    callback_types[cb_name] = callback_types.get(cb_name, 0) + 1

            print("Callback types:")
            for cb_type, count in callback_types.items():
                print(f"  {cb_type}: {count}")

        # Try to find common attributes in objects
        found_attributes = []
        for obj in gc.get_objects():
            if not isinstance(obj, (asyncio.Future, asyncio.Task)) and hasattr(obj, '__dict__'):
                try:
                    for key, value in obj.__dict__.items():
                        if value is fut:
                            obj_type = type(obj).__name__
                            found_attributes.append((obj_type, key, obj))
                            if len(found_attributes) >= 5:  # Limit search to avoid too much output
                                break
                except Exception:
                    pass  # Skip objects with problematic __dict__ access

        print("Found in these objects:")
        for obj_type, attr_name, obj in found_attributes:
            print(f"  {obj_type}.{attr_name} = {obj}")

        # Print sample of waiting tasks
        print(f"Sample of waiting tasks:")
        for i, task in enumerate(waiting_tasks[:5]):  # Show up to 5 tasks
            print(f"  {i + 1}. {task.get_name()} - {task}")

            # Try to get the stack for each task
            try:
                if hasattr(task, '_coro'):
                    coro = task._coro
                    if hasattr(coro, 'cr_frame'):
                        frame = coro.cr_frame
                        if frame:
                            file_name = os.path.basename(frame.f_code.co_filename)
                            line_no = frame.f_lineno
                            func_name = frame.f_code.co_name
                            print(f"      At: {file_name}:{line_no} in {func_name}")
                    elif hasattr(coro, 'gi_frame'):
                        frame = coro.gi_frame
                        if frame:
                            file_name = os.path.basename(frame.f_code.co_filename)
                            line_no = frame.f_lineno
                            func_name = frame.f_code.co_name
                            print(f"      At: {file_name}:{line_no} in {func_name}")
            except Exception as e:
                print(f"      Error getting stack: {e}")


# Function to call from anywhere in your code
def analyze_futures():
    """Call this from any synchronous code to analyze futures."""
    # If we're already in the event loop thread, run directly
    try:
        loop = asyncio.get_running_loop()
        if threading.current_thread() is threading.main_thread():
            find_future_source_sync()
            return
    except RuntimeError:
        pass  # No running event loop

    # Otherwise, schedule it to run in the event loop
    loop = asyncio.get_event_loop()
    if not loop.is_running():
        loop.run_until_complete(asyncio.sleep(0))  # Ensure loop is running
    loop.call_soon_threadsafe(find_future_source_sync)
