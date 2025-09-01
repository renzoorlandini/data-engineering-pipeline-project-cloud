# pipeline.py
import subprocess

def run_step(script, description):
    print(f"\n=== {description} ===")
    try:
        subprocess.run(["python", script], check=True)
        print(f"--- {description} completed successfully ---")
    except subprocess.CalledProcessError as e:
        print(f"!!! {description} failed with error: {e}")
        raise

def main():
    print("Starting full ETL pipeline...\n")
    run_step("main.py", "Step 1: Load raw data")
    run_step("create_dimensions.py", "Step 2: Create dim_locations")
    run_step("transform.py", "Step 3: Build master_table (placeholder)")
    print("\nETL Pipeline finished successfully.")

if __name__ == "__main__":
    main()
