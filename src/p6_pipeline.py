"""
Task P6: Complete Pipeline - Labels & Sanity Plots

This script orchestrates the full P6 workflow:
  1. Load features from P2 output
  2. Generate regime labels (volatility buckets + risk regimes)
  3. Generate sanity check plots and report

Run this after P2 completes.
"""

import sys
from pathlib import Path

# Add src to path
src_dir = Path(__file__).parent
sys.path.insert(0, str(src_dir))

from labels.make_labels import make_labels
from reports.sanity_plots import generate_sanity_plots


def run_p6_pipeline():
    """Execute the full P6 pipeline."""
    print("\n" + "=" * 70)
    print("STARTING TASK P6: LABELS & SANITY PLOTS")
    print("=" * 70)
    
    # Step 1: Generate labels
    print("\n[STEP 1/2] Generating regime labels...")
    try:
        labels_df = make_labels()
        print("✅ Label generation complete")
    except Exception as e:
        print(f"❌ Label generation failed: {e}")
        return False
    
    # Step 2: Generate sanity plots
    print("\n[STEP 2/2] Generating sanity plots and report...")
    try:
        generate_sanity_plots()
        print("✅ Sanity plots generation complete")
    except Exception as e:
        print(f"❌ Sanity plots generation failed: {e}")
        return False
    
    print("\n" + "=" * 70)
    print("✅ TASK P6 COMPLETE - Ready for handoff to P7")
    print("=" * 70)
    print("\nOutputs:")
    print("  - data/processed/labels_prelim.csv")
    print("  - reports/figures/regime_timeline.png")
    print("  - reports/figures/regime_stats.png")
    print("  - data/processed/f2_acceptance.md")
    print()
    
    return True


if __name__ == "__main__":
    success = run_p6_pipeline()
    sys.exit(0 if success else 1)
