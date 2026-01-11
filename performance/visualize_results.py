#!/usr/bin/env python3
"""
Performance Visualization Script for PySpark Optimization Analysis

This script reads performance results (JSON format) and generates
three comprehensive visualization charts:
1. Grouped Bar Chart with Error Bars - Average Runtime Comparison
2. Box Plot - Statistical Distribution of Runtimes
3. Speedup Factor Chart - Performance Improvement vs. Baseline

Usage:
    python visualize_results.py <path_to_results_json>
    
Example:
    python visualize_results.py performance_results.json
"""

import json
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Set style for professional-looking charts
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)
plt.rcParams['font.size'] = 10

# Color palette for consistent visualization
COLOR_PALETTE = {
    'baseline': '#d62728',      # Red - worst case
    'single': '#ff7f0e',        # Orange - single optimization
    'combined': '#2ca02c',      # Green - best case
    'neutral': '#1f77b4'        # Blue - neutral
}


class PerformanceVisualizer:
    """Handles all performance visualization tasks"""
    
    def __init__(self, results_file):
        """
        Initialize visualizer with results data.

        Args:
            results_file: Path to JSON file containing performance results
        """
        self.results_file = Path(results_file)
        self.data = self._load_results()
        self.df = self._prepare_dataframe()
        self.summary_stats = self._calculate_summary_stats()
        
    def _load_results(self):
        """
        Load results from JSON file.

        Returns:
            List of dictionaries containing performance data
        """
        try:
            with open(self.results_file, 'r') as f:
                data = json.load(f)
            print(f"Loaded results from {self.results_file}")
            print(f"  Total runs: {len(data)}")
            return data
        except FileNotFoundError:
            print(f"X Error: Results file '{self.results_file}' not found")
            sys.exit(1)
        except json.JSONDecodeError:
            print(f"X Error: Invalid JSON format in '{self.results_file}'")
            sys.exit(1)
    
    def _prepare_dataframe(self):
        """
        Convert results to pandas DataFrame for analysis.

        Returns:
            DataFrame with normalized configuration names and runtime data
        """
        records = []
        
        for result in self.data:
            config = result.get('config', {})
            config_name = self._get_config_name(config)
            
            records.append({
                'configuration': config_name,
                'runtime': result.get('runtime', 0),
                'iteration': result.get('iteration', 0),
                'config_dict': config
            })
        
        df = pd.DataFrame(records)
        print(f"Prepared DataFrame with {len(df)} records")
        return df
    
    def _get_config_name(self, config):
        """
        Generate readable configuration name from config dictionary.

        Args:
            config: Dictionary of optimization flags

        Returns:
            Human-readable string describing the configuration
        """
        if not any(config.values()):
            return "Baseline (No Opts)"
        
        active = []
        if config.get('USE_CACHING'):
            active.append('Cache')
        if config.get('USE_BROADCAST_JOIN'):
            active.append('Broadcast')
        if config.get('EARLY_COLUMN_PROJECTION'):
            active.append('EarlyProj')
        if config.get('OPTIMIZE_PARTITIONING'):
            active.append('Partition')
        if config.get('OPTIMIZED_FILTER_ORDER'):
            active.append('FilterOpt')
        
        if len(active) == 5:
            return "All Optimizations"
        elif len(active) > 2:
            return " + ".join(active[:2]) + f" +{len(active)-2}"
        else:
            return " + ".join(active)
    
    def _calculate_summary_stats(self):
        """
        Calculate summary statistics for each configuration.

        Returns:
            Dictionary mapping configuration names to their statistics
        """
        summary = {}
        
        for config in self.df['configuration'].unique():
            runtimes = self.df[self.df['configuration'] == config]['runtime'].values
            
            baseline_runtime = self.df[self.df['configuration'] == 'Baseline (No Opts)']['runtime'].mean()
            
            summary[config] = {
                'mean': np.mean(runtimes),
                'median': np.median(runtimes),
                'std': np.std(runtimes),
                'min': np.min(runtimes),
                'max': np.max(runtimes),
                'count': len(runtimes),
                'speedup': baseline_runtime / np.mean(runtimes) if np.mean(runtimes) > 0 else 0
            }
        
        print("Calculated summary statistics")
        return summary
    
    def plot_grouped_bar_chart(self, output_file='1_avg_runtime_comparison.png'):
        """
        Create Grouped Bar Chart with Error Bars.
        Shows average runtime for each configuration with standard deviation.

        Args:
            output_file: Path where the chart will be saved
        """
        print(f"\nCreating Grouped Bar Chart...")

        fig, ax = plt.subplots(figsize=(14, 7))
        
        configs = sorted(self.summary_stats.keys())
        means = [self.summary_stats[c]['mean'] for c in configs]
        stds = [self.summary_stats[c]['std'] for c in configs]
        
        # Determine colors based on performance
        colors = []
        for i, config in enumerate(configs):
            if config == "Baseline (No Opts)":
                colors.append(COLOR_PALETTE['baseline'])
            elif config == "All Optimizations":
                colors.append(COLOR_PALETTE['combined'])
            elif len(config.split(' + ')) <= 2:
                colors.append(COLOR_PALETTE['single'])
            else:
                colors.append(COLOR_PALETTE['neutral'])
        
        bars = ax.bar(range(len(configs)), means, yerr=stds, capsize=5, 
                      color=colors, alpha=0.8, edgecolor='black', linewidth=1.5,
                      error_kw={'linewidth': 2, 'ecolor': 'gray'})
        
        # Add value labels on bars
        for i, (bar, mean, std) in enumerate(zip(bars, means, stds)):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{mean:.2f}s\n+/-{std:.2f}s',
                   ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        # Formatting
        ax.set_ylabel('Runtime (seconds)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Configuration', fontsize=12, fontweight='bold')
        ax.set_title('Average Runtime Comparison Across Optimization Configurations\n(Error bars show +/-1 standard deviation)',
                     fontsize=13, fontweight='bold', pad=20)
        ax.set_xticks(range(len(configs)))
        ax.set_xticklabels(configs, rotation=45, ha='right')
        ax.grid(axis='y', alpha=0.3)
        ax.set_ylim(bottom=0)
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")
        plt.close()
    
    def plot_box_plot(self, output_file='2_runtime_distribution_boxplot.png'):
        """
        Create Box Plot.
        Shows distribution of runtimes across iterations for each configuration.
        Reveals variance and outliers in measurements.

        Args:
            output_file: Path where the chart will be saved
        """
        print(f"Creating Box Plot...")

        fig, ax = plt.subplots(figsize=(14, 7))
        
        # Sort configurations for consistent ordering
        configs = sorted(self.df['configuration'].unique())
        data_for_box = [self.df[self.df['configuration'] == c]['runtime'].values for c in configs]
        
        bp = ax.boxplot(data_for_box, labels=configs, patch_artist=True,
                        notch=False, showfliers=True,
                        boxprops=dict(facecolor='lightblue', alpha=0.7),
                        medianprops=dict(color='red', linewidth=2),
                        whiskerprops=dict(linewidth=1.5),
                        capprops=dict(linewidth=1.5),
                        flierprops=dict(marker='o', markerfacecolor='red', markersize=6, alpha=0.5))
        
        # Color boxes by performance tier
        for patch, config in zip(bp['boxes'], configs):
            if config == "Baseline (No Opts)":
                patch.set_facecolor(COLOR_PALETTE['baseline'])
            elif config == "All Optimizations":
                patch.set_facecolor(COLOR_PALETTE['combined'])
            elif len(config.split(' + ')) <= 2:
                patch.set_facecolor(COLOR_PALETTE['single'])
            else:
                patch.set_facecolor(COLOR_PALETTE['neutral'])
            patch.set_alpha(0.7)
        
        # Formatting
        ax.set_ylabel('Runtime (seconds)', fontsize=12, fontweight='bold')
        ax.set_xlabel('Configuration', fontsize=12, fontweight='bold')
        ax.set_title('Runtime Distribution: Box Plot Comparison\n(Box shows IQR, line shows median, dots show outliers)', 
                     fontsize=13, fontweight='bold', pad=20)
        ax.set_xticklabels(configs, rotation=45, ha='right')
        ax.grid(axis='y', alpha=0.3)
        
        # Add legend for box plot elements
        legend_elements = [
            plt.Line2D([0], [0], color='red', linewidth=2, label='Median'),
            plt.Rectangle((0, 0), 1, 1, fc='lightblue', alpha=0.7, label='IQR (middle 50%)'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='red', 
                      markersize=6, alpha=0.5, label='Outliers')
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=10)
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")
        plt.close()
    
    def plot_speedup_chart(self, output_file='3_speedup_factor_comparison.png'):
        """
        Create Speedup Factor Chart.
        Shows performance improvement relative to baseline.
        Speedup = Baseline Runtime / Configuration Runtime

        Args:
            output_file: Path where the chart will be saved
        """
        print(f"Creating Speedup Factor Chart...")

        fig, ax = plt.subplots(figsize=(14, 7))
        
        # Sort by speedup factor
        sorted_stats = sorted(self.summary_stats.items(), 
                            key=lambda x: x[1]['speedup'], reverse=True)
        configs = [item[0] for item in sorted_stats]
        speedups = [item[1]['speedup'] for item in sorted_stats]
        
        # Determine colors - speedup >= 1.0 is improvement
        colors = []
        for speedup in speedups:
            if speedup == 1.0:
                colors.append(COLOR_PALETTE['baseline'])
            elif speedup > 1.5:
                colors.append(COLOR_PALETTE['combined'])  # Major improvement
            elif speedup >= 1.0:
                colors.append(COLOR_PALETTE['single'])    # Minor improvement
            else:
                colors.append('#d62728')  # Red for regression
        
        # Create horizontal bar chart
        bars = ax.barh(range(len(configs)), speedups, color=colors, 
                       alpha=0.8, edgecolor='black', linewidth=1.5)
        
        # Add reference line at 1.0x (no speedup)
        ax.axvline(x=1.0, color='black', linestyle='--', linewidth=2, alpha=0.7, label='Baseline (1.0x)')
        
        # Add value labels on bars
        for i, (bar, speedup) in enumerate(zip(bars, speedups)):
            width = bar.get_width()
            improvement_pct = (speedup - 1.0) * 100
            ax.text(width + 0.05, bar.get_y() + bar.get_height()/2.,
                   f'{speedup:.2f}x ({improvement_pct:+.1f}%)',
                   ha='left', va='center', fontsize=9, fontweight='bold')
        
        # Formatting
        ax.set_xlabel('Speedup Factor (Baseline / Configuration Runtime)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Configuration', fontsize=12, fontweight='bold')
        ax.set_title('Performance Improvement: Speedup Factor vs. Baseline\n(1.0x = same as baseline, >1.0x = faster, <1.0x = slower)', 
                     fontsize=13, fontweight='bold', pad=20)
        ax.set_yticks(range(len(configs)))
        ax.set_yticklabels(configs)
        ax.set_xlim(left=0)
        ax.grid(axis='x', alpha=0.3)
        ax.legend(loc='lower right', fontsize=10)
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Saved: {output_file}")
        plt.close()
    
    def print_summary_table(self):
        """Print summary statistics table to console"""
        print("\n" + "="*100)
        print("PERFORMANCE SUMMARY STATISTICS")
        print("="*100)
        print(f"{'Configuration':<30} {'Mean (s)':<12} {'Std (s)':<12} {'Min (s)':<12} {'Max (s)':<12} {'Speedup':<12}")
        print("-"*100)
        
        baseline_mean = self.summary_stats.get('Baseline (No Opts)', {}).get('mean', float('inf'))
        
        for config in sorted(self.summary_stats.keys()):
            stats = self.summary_stats[config]
            speedup = stats['speedup']
            
            # Color coding for speedup
            speedup_str = f"{speedup:.2f}x"
            if speedup == 1.0:
                speedup_str += " (baseline)"
            elif speedup > 1.5:
                speedup_str += " ** (excellent)"
            elif speedup > 1.2:
                speedup_str += " * (good)"
            elif speedup > 1.0:
                speedup_str += " (minor improvement)"
            elif speedup < 1.0:
                speedup_str += " ! (regression)"

            print(f"{config:<30} {stats['mean']:<12.3f} {stats['std']:<12.3f} {stats['min']:<12.3f} {stats['max']:<12.3f} {speedup_str:<12}")
        
        print("="*100 + "\n")
    
    def generate_all_charts(self):
        """Generate all three visualization charts"""
        print("\n" + "="*60)
        print("GENERATING PERFORMANCE VISUALIZATIONS")
        print("="*60)
        
        self.print_summary_table()
        
        self.plot_grouped_bar_chart()
        self.plot_box_plot()
        self.plot_speedup_chart()
        
        print("\n" + "="*60)
        print("ALL VISUALIZATIONS GENERATED SUCCESSFULLY")
        print("="*60)
        print("\nGenerated files:")
        print("  1. 1_avg_runtime_comparison.png")
        print("  2. 2_runtime_distribution_boxplot.png")
        print("  3. 3_speedup_factor_comparison.png")


def main():
    """Main entry point for command-line usage"""
    if len(sys.argv) < 2:
        print("Usage: python visualize_results.py <path_to_results_json>")
        print("\nExample:")
        print("  python visualize_results.py performance_results.json")
        sys.exit(1)
    
    results_file = sys.argv[1]
    
    visualizer = PerformanceVisualizer(results_file)
    visualizer.generate_all_charts()


if __name__ == "__main__":
    main()
