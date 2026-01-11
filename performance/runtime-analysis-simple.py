# performance_wrapper.py
import subprocess
import time
import sys

def measure_script_performance(script_path, iterations=3):
    """
    Measure the performance of a Spark script by running it multiple times.

    Args:
        script_path: Path to the Spark script to be measured
        iterations: Number of times to run the script (default: 3)

    Returns:
        List of dictionaries containing runtime metrics for each iteration
    """
    results = []

    for i in range(iterations):
        start = time.time()
        result = subprocess.run(
            ['spark-submit', script_path],
            capture_output=True,
            text=True
        )
        end = time.time()

        runtime = end - start
        results.append({
            'iteration': i + 1,
            'runtime': runtime,
            'exit_code': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr
        })

    return results
