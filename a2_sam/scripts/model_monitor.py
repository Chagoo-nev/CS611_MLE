import argparse
import os
import json
import pandas as pd
import numpy as np
import pickle
import glob
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix, roc_curve
import warnings
warnings.filterwarnings('ignore')


def load_gold_data(snapshot_date_str, gold_base_dir="datamart/gold/"):
    """
    Load gold feature and label data for given snapshot date
    """
    part_suffix = snapshot_date_str.replace('-', '_')
    
    # Load features and labels
    feature_path = f"{gold_base_dir}feature_store/gold_feature_store_{part_suffix}.parquet"
    label_path = f"{gold_base_dir}label_store/gold_label_store_{part_suffix}.parquet"
    
    try:
        df_features = pd.read_parquet(feature_path)
        df_labels = pd.read_parquet(label_path)
        
        # Join features and labels
        modeling_data = df_features.merge(
            df_labels[['Customer_ID', 'target']], 
            on='Customer_ID', 
            how='inner'
        )
        
        # Remove non-feature columns
        exclude_cols = ['Customer_ID', 'snapshot_date', 'target']
        feature_columns = [col for col in modeling_data.columns if col not in exclude_cols]
        
        X = modeling_data[feature_columns]
        y = modeling_data['target']
        
        return X, y, len(modeling_data)
        
    except Exception as e:
        print(f"Error loading data for {snapshot_date_str}: {e}")
        return None, None, 0


def load_model(model_path):
    """
    Load a trained model from pickle file
    """
    try:
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        return model
    except Exception as e:
        print(f"Error loading model {model_path}: {e}")
        return None


def evaluate_model(model, X, y, model_name, model_date, eval_date):
    """
    Evaluate model performance and return metrics
    """
    try:
        # Make predictions
        y_pred_proba = model.predict_proba(X)[:, 1] if hasattr(model, 'predict_proba') else model.predict(X)
        y_pred = (y_pred_proba > 0.5).astype(int)
        
        # Calculate metrics
        auc_score = roc_auc_score(y, y_pred_proba)
        
        # Classification report
        report = classification_report(y, y_pred, output_dict=True)
        
        # Confusion matrix
        cm = confusion_matrix(y, y_pred)
        
        # Additional metrics
        precision = report['1']['precision']
        recall = report['1']['recall']
        f1_score = report['1']['f1-score']
        
        # Prediction distribution
        pred_stats = {
            'mean_pred_proba': float(np.mean(y_pred_proba)),
            'std_pred_proba': float(np.std(y_pred_proba)),
            'min_pred_proba': float(np.min(y_pred_proba)),
            'max_pred_proba': float(np.max(y_pred_proba))
        }
        
        metrics = {
            'model_name': model_name,
            'model_date': model_date,
            'evaluation_date': eval_date,
            'sample_size': len(y),
            'actual_positive_rate': float(np.mean(y)),
            'auc_score': float(auc_score),
            'precision': float(precision),
            'recall': float(recall),
            'f1_score': float(f1_score),
            'confusion_matrix': cm.tolist(),
            'prediction_stats': pred_stats,
            'classification_report': report
        }
        
        return metrics
        
    except Exception as e:
        print(f"Error evaluating {model_name} model: {e}")
        return None


def find_available_models(model_bank_dir, current_date_str):
    """
    Find all available models before current date
    """
    current_date = datetime.strptime(current_date_str, "%Y-%m-%d")
    
    # Find RF models
    rf_models = []
    rf_pattern = os.path.join(model_bank_dir, "rf_model_*.pkl")
    for model_path in glob.glob(rf_pattern):
        filename = os.path.basename(model_path)
        # Extract date from filename: rf_model_2023_04_01.pkl
        date_part = filename.replace('rf_model_', '').replace('.pkl', '')
        try:
            model_date = datetime.strptime(date_part.replace('_', '-'), "%Y-%m-%d")
            if model_date < current_date:  # Only use models trained before current date
                rf_models.append((model_path, model_date.strftime("%Y-%m-%d")))
        except:
            continue
    
    # Find LGB models  
    lgb_models = []
    lgb_pattern = os.path.join(model_bank_dir, "lgb_model_*.pkl")
    for model_path in glob.glob(lgb_pattern):
        filename = os.path.basename(model_path)
        # Extract date from filename: lgb_model_2023_04_01.pkl
        date_part = filename.replace('lgb_model_', '').replace('.pkl', '')
        try:
            model_date = datetime.strptime(date_part.replace('_', '-'), "%Y-%m-%d")
            if model_date < current_date:  # Only use models trained before current date
                lgb_models.append((model_path, model_date.strftime("%Y-%m-%d")))
        except:
            continue
    
    return sorted(rf_models, key=lambda x: x[1]), sorted(lgb_models, key=lambda x: x[1])


def compare_models(rf_metrics, lgb_metrics):
    """
    Compare performance between RF and LGB models
    """
    if not rf_metrics or not lgb_metrics:
        return None
    
    comparison = {
        'evaluation_date': rf_metrics['evaluation_date'],
        'rf_model_date': rf_metrics['model_date'],
        'lgb_model_date': lgb_metrics['model_date'],
        'metrics_comparison': {
            'auc_score': {
                'rf': rf_metrics['auc_score'],
                'lgb': lgb_metrics['auc_score'],
                'winner': 'RF' if rf_metrics['auc_score'] > lgb_metrics['auc_score'] else 'LGB',
                'difference': abs(rf_metrics['auc_score'] - lgb_metrics['auc_score'])
            },
            'precision': {
                'rf': rf_metrics['precision'],
                'lgb': lgb_metrics['precision'],
                'winner': 'RF' if rf_metrics['precision'] > lgb_metrics['precision'] else 'LGB',
                'difference': abs(rf_metrics['precision'] - lgb_metrics['precision'])
            },
            'recall': {
                'rf': rf_metrics['recall'],
                'lgb': lgb_metrics['recall'],
                'winner': 'RF' if rf_metrics['recall'] > lgb_metrics['recall'] else 'LGB',
                'difference': abs(rf_metrics['recall'] - lgb_metrics['recall'])
            },
            'f1_score': {
                'rf': rf_metrics['f1_score'],
                'lgb': lgb_metrics['f1_score'],
                'winner': 'RF' if rf_metrics['f1_score'] > lgb_metrics['f1_score'] else 'LGB',
                'difference': abs(rf_metrics['f1_score'] - lgb_metrics['f1_score'])
            }
        }
    }
    
    return comparison


def monitor_models(snapshot_date_str, model_bank_dir, output_dir):
    """
    Main monitoring function
    """
    print(f"🔍 Model Monitoring for {snapshot_date_str}")
    print("=" * 50)
    
    # Load current month's data
    X, y, sample_size = load_gold_data(snapshot_date_str)
    
    if X is None or len(X) == 0:
        print(f"❌ No data available for {snapshot_date_str}")
        return
    
    print(f"📊 Loaded evaluation data: {sample_size} samples, {len(X.columns)} features")
    print(f"📈 Actual positive rate: {np.mean(y):.3f}")
    
    # Find available models
    rf_models, lgb_models = find_available_models(model_bank_dir, snapshot_date_str)
    
    print(f"\n🔍 Found {len(rf_models)} RF models and {len(lgb_models)} LGB models")
    
    # Evaluate latest RF model
    rf_metrics = None
    if rf_models:
        latest_rf_path, latest_rf_date = rf_models[-1]  # Most recent model
        print(f"\n🌲 Evaluating latest RF model: {latest_rf_date}")
        
        rf_model = load_model(latest_rf_path)
        if rf_model:
            rf_metrics = evaluate_model(rf_model, X, y, "RandomForest", latest_rf_date, snapshot_date_str)
            if rf_metrics:
                print(f"   AUC: {rf_metrics['auc_score']:.4f}")
                print(f"   Precision: {rf_metrics['precision']:.4f}")
                print(f"   Recall: {rf_metrics['recall']:.4f}")
    
    # Evaluate latest LGB model
    lgb_metrics = None
    if lgb_models:
        latest_lgb_path, latest_lgb_date = lgb_models[-1]  # Most recent model
        print(f"\n🚀 Evaluating latest LGB model: {latest_lgb_date}")
        
        lgb_model = load_model(latest_lgb_path)
        if lgb_model:
            lgb_metrics = evaluate_model(lgb_model, X, y, "LightGBM", latest_lgb_date, snapshot_date_str)
            if lgb_metrics:
                print(f"   AUC: {lgb_metrics['auc_score']:.4f}")
                print(f"   Precision: {lgb_metrics['precision']:.4f}")
                print(f"   Recall: {lgb_metrics['recall']:.4f}")
    
    # Compare models
    comparison = None
    if rf_metrics and lgb_metrics:
        comparison = compare_models(rf_metrics, lgb_metrics)
        print(f"\n⚖️  Model Comparison:")
        for metric_name, metric_data in comparison['metrics_comparison'].items():
            winner = metric_data['winner']
            diff = metric_data['difference']
            print(f"   {metric_name.upper()}: {winner} wins by {diff:.4f}")
    
    # Save monitoring results
    os.makedirs(output_dir, exist_ok=True)
    
    monitor_results = {
        'monitoring_date': snapshot_date_str,
        'evaluation_summary': {
            'sample_size': sample_size,
            'actual_positive_rate': float(np.mean(y)),
            'features_count': len(X.columns)
        },
        'rf_performance': rf_metrics,
        'lgb_performance': lgb_metrics,
        'model_comparison': comparison,
        'timestamp': datetime.now().isoformat()
    }
    
    # Save individual model metrics
    part_suffix = snapshot_date_str.replace('-', '_')
    
    if rf_metrics:
        rf_output_path = os.path.join(output_dir, f"rf_monitor_{part_suffix}.json")
        with open(rf_output_path, 'w') as f:
            json.dump({'rf_metrics': rf_metrics, 'evaluation_summary': monitor_results['evaluation_summary']}, f, indent=2)
        print(f"\n💾 RF monitoring saved to: {rf_output_path}")
    
    if lgb_metrics:
        lgb_output_path = os.path.join(output_dir, f"lgb_monitor_{part_suffix}.json")
        with open(lgb_output_path, 'w') as f:
            json.dump({'lgb_metrics': lgb_metrics, 'evaluation_summary': monitor_results['evaluation_summary']}, f, indent=2)
        print(f"💾 LGB monitoring saved to: {lgb_output_path}")
    
    # Save combined monitoring report
    combined_output_path = os.path.join(output_dir, f"model_monitor_{part_suffix}.json")
    with open(combined_output_path, 'w') as f:
        json.dump(monitor_results, f, indent=2)
    print(f"💾 Combined monitoring report saved to: {combined_output_path}")
    
    print(f"\n✅ Model monitoring completed for {snapshot_date_str}")


def main():
    """
    Main function
    """
    parser = argparse.ArgumentParser(description='Monitor model performance')
    parser.add_argument('--snapshotdate', required=True, help='Snapshot date in YYYY-MM-DD format')
    parser.add_argument('--model_bank_dir', default='scripts/model_bank/', help='Model bank directory')
    parser.add_argument('--output_dir', default='datamart/monitoring/', help='Monitoring output directory')
    
    args = parser.parse_args()
    
    try:
        monitor_models(args.snapshotdate, args.model_bank_dir, args.output_dir)
    except Exception as e:
        print(f"❌ Error in model monitoring: {e}")
        raise


if __name__ == "__main__":
    main()