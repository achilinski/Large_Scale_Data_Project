# performance_evaluator.py
import subprocess
import time
import json
import sys
from itertools import product

# Import visualization functionality
try:
    from visualize_results import PerformanceVisualizer
except ImportError:
    print("Warning: Could not import visualization module. Install required packages:")
    print("  pip install matplotlib seaborn pandas numpy")
    PerformanceVisualizer = None

def run_configuration(config, iteration=1):
    """
    Run the PySpark script with specific configuration.

    Args:
        config: Dictionary containing configuration flags
        iteration: Current iteration number

    Returns:
        Dictionary with runtime metrics and configuration details
    """

    # Create environment variables for configuration
    env_vars = {f"{key}": str(value) for key, value in config.items()}

    # Modify the main script to read from environment or pass as arguments
    # For simplicity, we'll use file modification

    # Read original script
    with open('q9-for-performance.py', 'r') as f:
        script_content = f.read()

    # Replace configuration values
    for key, value in config.items():
        # Find and replace the configuration line
        script_content = script_content.replace(
            f"{key} = True", f"{key} = {value}"
        ).replace(
            f"{key} = False", f"{key} = {value}"
        )

    # Write temporary script
    temp_script = f'q9_temp_config_{iteration}.py'
    with open(temp_script, 'w') as f:
        f.write(script_content)

    # Run the script and measure time
    start_time = time.time()
    result = subprocess.run(
        ['spark-submit', '--master', 'local[8]', temp_script],
        capture_output=True,
        text=True
    )
    end_time = time.time()

    runtime = end_time - start_time

    return {
        'config': config,
        'iteration': iteration,
        'runtime': runtime,
        'exit_code': result.returncode,
        'stdout': result.stdout[-500:],  # Last 500 chars
        'stderr': result.stderr[-500:] if result.returncode != 0 else ''
    }

def main():
    """
    Main function to run performance evaluation across different configurations.
    Tests individual optimizations and combinations to identify performance impact.
    """
    # Define configurations to test
    configurations = [
        # Baseline: all optimizations off
        {
            'USE_CACHING': False,
            'USE_BROADCAST_JOIN': False,
            'EARLY_COLUMN_PROJECTION': False,
            'OPTIMIZE_PARTITIONING': False,
            'OPTIMIZED_FILTER_ORDER': False
        },
        # Test each optimization individually
        {'USE_CACHING': True, 'USE_BROADCAST_JOIN': False, 'EARLY_COLUMN_PROJECTION': False,
         'OPTIMIZE_PARTITIONING': False, 'OPTIMIZED_FILTER_ORDER': False},
        {'USE_CACHING': False, 'USE_BROADCAST_JOIN': True, 'EARLY_COLUMN_PROJECTION': False,
         'OPTIMIZE_PARTITIONING': False, 'OPTIMIZED_FILTER_ORDER': False},
        {'USE_CACHING': False, 'USE_BROADCAST_JOIN': False, 'EARLY_COLUMN_PROJECTION': True,
         'OPTIMIZE_PARTITIONING': False, 'OPTIMIZED_FILTER_ORDER': False},
        {'USE_CACHING': False, 'USE_BROADCAST_JOIN': False, 'EARLY_COLUMN_PROJECTION': False,
         'OPTIMIZE_PARTITIONING': True, 'OPTIMIZED_FILTER_ORDER': False},
        {'USE_CACHING': False, 'USE_BROADCAST_JOIN': False, 'EARLY_COLUMN_PROJECTION': False,
         'OPTIMIZE_PARTITIONING': False, 'OPTIMIZED_FILTER_ORDER': True},
        # All optimizations combined
        {
            'USE_CACHING': True,
            'USE_BROADCAST_JOIN': True,
            'EARLY_COLUMN_PROJECTION': True,
            'OPTIMIZE_PARTITIONING': True,
            'OPTIMIZED_FILTER_ORDER': True
        }
    ]

    results = []
    iterations = 3  # Run each configuration 3 times for statistical significance

    for idx, config in enumerate(configurations):
        print(f"\n{'='*60}")
        print(f"Testing Configuration {idx + 1}/{len(configurations)}")
        print(f"{'='*60}")
        print(json.dumps(config, indent=2))

        for iteration in range(iterations):
            print(f"\n  Iteration {iteration + 1}/{iterations}...")
            result = run_configuration(config, iteration)
            results.append(result)
            print(f"  Runtime: {result['runtime']:.2f}s")

    # Save results
    with open('performance_results.json', 'w') as f:
        json.dump(results, f, indent=2)

    # Print summary
    print("\n" + "="*60)
    print("PERFORMANCE SUMMARY")
    print("="*60)

    for idx, config in enumerate(configurations):
        config_results = [r for r in results if r['config'] == config]
        runtimes = [r['runtime'] for r in config_results]
        avg_runtime = sum(runtimes) / len(runtimes)

        print(f"\nConfig {idx + 1}: {config}")
        print(f"  Avg Runtime: {avg_runtime:.2f}s")
        print(f"  Min Runtime: {min(runtimes):.2f}s")
        print(f"  Max Runtime: {max(runtimes):.2f}s")

    # Generate visualizations automatically after analysis
    print("\n" + "="*60)
    print("STARTING VISUALIZATION GENERATION")
    print("="*60)

    if PerformanceVisualizer is not None:
        try:
            visualizer = PerformanceVisualizer('performance_results.json')
            visualizer.generate_all_charts()
        except Exception as e:
            print(f"X Error generating visualizations: {e}")
            print("Results saved to performance_results.json - you can generate charts manually with:")
            print("  python visualize_results.py performance_results.json")
    else:
        print("Visualization module not available. Generate charts manually with:")
        print("  python visualize_results.py performance_results.json")

if __name__ == "__main__":
    main()
