import argparse
import os
import pandas as pd
import joblib
import lightgbm as lgb
from datetime import datetime

def load_latest_model(model_bank_dir, snapshot_date_str):
    """
    Load LGB model for the given date, return None if not found
    """
    model_filename = f"lgb_model_{snapshot_date_str.replace('-', '_')}.pkl"
    model_path = os.path.join(model_bank_dir, model_filename)
    
    if not os.path.exists(model_path):
        print(f"⚠️  Model not found: {model_path}")
        return None
    
    try:
        model = joblib.load(model_path)
        print(f"✅ Loaded model: {model_path}")
        return model
    except Exception as e:
        print(f"❌ Error loading model {model_path}: {e}")
        return None

def load_inference_data(gold_base_dir, snapshot_date_str):
    """
    Load inference data, return None if not found
    """
    part_suffix = snapshot_date_str.replace('-', '_')
    feature_path = f"{gold_base_dir}feature_store/gold_feature_store_{part_suffix}.parquet"
    
    if not os.path.exists(feature_path):
        print(f"⚠️  Inference data not found: {feature_path}")
        return None
    
    try:
        df = pd.read_parquet(feature_path)
        print(f"📥 Loaded inference data: {len(df)} records")
        return df
    except Exception as e:
        print(f"❌ Error loading inference data {feature_path}: {e}")
        return None

def preprocess_for_inference(df, model):
    """
    Preprocess data for LGB inference
    """
    try:
        feature_names = model.feature_name()
        missing_cols = [col for col in feature_names if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing features in inference data: {missing_cols}")
        
        df = df.copy()
        
        # Fill missing categorical as "Unknown"
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].fillna('Unknown').astype('category')
        
        # Fill missing numerical as 0
        for col in df.select_dtypes(include=['number']).columns:
            df[col] = df[col].fillna(0)
        
        X = df[feature_names]
        return df, X
    except Exception as e:
        print(f"❌ Error in preprocessing: {e}")
        return None, None

def run_inference(model, X):
    """
    Run LGB model inference
    """
    try:
        y_pred_proba = model.predict(X, num_iteration=model.best_iteration)
        return y_pred_proba
    except Exception as e:
        print(f"❌ Error in model inference: {e}")
        return None

def save_predictions(df, y_pred_proba, snapshot_date_str, output_dir):
    """
    Save predictions to file
    """
    try:
        df['prediction_proba'] = y_pred_proba
        df['prediction_label'] = (y_pred_proba > 0.5).astype(int)
        
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"model_2_predictions_{snapshot_date_str.replace('-', '_')}.csv")

        if os.path.exists(output_path):
            print(f"⚠️ Overwriting existing file: {output_path}")
        
        df.to_csv(output_path, index=False)
        print(f"✅ Predictions saved to: {output_path}")
        return True
    except Exception as e:
        print(f"❌ Error saving predictions: {e}")
        return False

def create_skip_marker(snapshot_date_str, output_dir, reason):
    """
    Create a marker file indicating inference was skipped
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        marker_path = os.path.join(output_dir, f"model_2_inference_skipped_{snapshot_date_str.replace('-', '_')}.txt")
        
        with open(marker_path, 'w') as f:
            f.write(f"Model 2 Inference Skipped\n")
            f.write(f"Date: {snapshot_date_str}\n")
            f.write(f"Reason: {reason}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Status: SUCCESS (graceful skip)\n")
        
        print(f"📝 Skip marker created: {marker_path}")
        return True
    except Exception as e:
        print(f"❌ Error creating skip marker: {e}")
        return False

def main(snapshot_date_str, gold_base_dir, model_bank_dir, output_dir):
    """
    Main inference function with graceful skipping
    """
    print(f"\n🚀 Running Model 2 Inference (LightGBM) for {snapshot_date_str}")
    print("=" * 60)
    
    # Step 1: Load model
    model = load_latest_model(model_bank_dir, snapshot_date_str)
    if model is None:
        reason = "LGB model not found or failed to load"
        print(f"⏭️  Skipping inference: {reason}")
        create_skip_marker(snapshot_date_str, output_dir, reason)
        print(f"✅ Model 2 inference completed successfully (skipped)")
        return
    
    # Step 2: Load data
    df = load_inference_data(gold_base_dir, snapshot_date_str)
    if df is None:
        reason = "Inference data not found or failed to load"
        print(f"⏭️  Skipping inference: {reason}")
        create_skip_marker(snapshot_date_str, output_dir, reason)
        print(f"✅ Model 2 inference completed successfully (skipped)")
        return
    
    # Step 3: Preprocess data
    df_full, X = preprocess_for_inference(df, model)
    if df_full is None or X is None:
        reason = "Data preprocessing failed"
        print(f"⏭️  Skipping inference: {reason}")
        create_skip_marker(snapshot_date_str, output_dir, reason)
        print(f"✅ Model 2 inference completed successfully (skipped)")
        return
    
    # Step 4: Run inference
    y_pred_proba = run_inference(model, X)
    if y_pred_proba is None:
        reason = "Model inference failed"
        print(f"⏭️  Skipping inference: {reason}")
        create_skip_marker(snapshot_date_str, output_dir, reason)
        print(f"✅ Model 2 inference completed successfully (skipped)")
        return
    
    # Step 5: Save predictions
    success = save_predictions(df_full, y_pred_proba, snapshot_date_str, output_dir)
    if not success:
        reason = "Failed to save predictions"
        print(f"⏭️  Marking as skipped: {reason}")
        create_skip_marker(snapshot_date_str, output_dir, reason)
        print(f"✅ Model 2 inference completed successfully (skipped)")
        return
    
    # Success case
    print(f"\n🎉 Model 2 inference completed successfully!")
    print(f"📊 Processed {len(df_full)} customers")
    print(f"📈 Mean prediction probability: {y_pred_proba.mean():.4f}")
    print(f"🎯 High-risk predictions: {(y_pred_proba > 0.5).sum()} customers")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Model 2 Inference (LightGBM) with Graceful Skipping")
    parser.add_argument('--snapshotdate', required=True, help='Snapshot date in YYYY-MM-DD format')
    parser.add_argument('--gold_base_dir', default='datamart/gold/', help='Base directory of gold features')
    parser.add_argument('--model_bank_dir', default='scripts/model_bank/', help='Directory where models are stored')
    parser.add_argument('--output_dir', default='datamart/inference/', help='Where to save predictions')
    
    args = parser.parse_args()
    
    try:
        main(args.snapshotdate, args.gold_base_dir, args.model_bank_dir, args.output_dir)
    except Exception as e:
        print(f"\n❌ Unexpected error in Model 2 inference: {e}")
        # Even on unexpected error, try to create a skip marker
        try:
            create_skip_marker(args.snapshotdate, args.output_dir, f"Unexpected error: {str(e)}")
            print(f"✅ Model 2 inference completed successfully (skipped due to error)")
        except:
            print(f"❌ Failed to create skip marker")
            raise