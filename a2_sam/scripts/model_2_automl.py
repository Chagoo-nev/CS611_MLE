import argparse
import os
import pandas as pd
import numpy as np
import joblib
import json
import glob
from datetime import datetime
import lightgbm as lgb
from sklearn.model_selection import train_test_split, cross_val_score, TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline as ImbPipeline
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')


def check_training_eligibility(snapshot_date_str):
    """
    Check if the snapshot date is eligible for model training
    
    Args:
        snapshot_date_str: Date in YYYY-MM-DD format
    
    Returns:
        eligible: Boolean indicating if training should proceed
        reason: String explaining the decision
    """
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    training_start_date = datetime(2023, 4, 1)  # Based on EDA results
    
    if snapshot_date < training_start_date:
        return False, f"Date {snapshot_date_str} is before minimum training date (2023-04-01). Insufficient risk samples."
    
    return True, f"Date {snapshot_date_str} is eligible for training."


def load_training_data(snapshot_date_str, gold_base_dir):
    """
    Load features and labels for model training using cumulative strategy
    LightGBM version - minimal preprocessing since LightGBM handles missing values
    
    Args:
        snapshot_date_str: Date in YYYY-MM-DD format
        gold_base_dir: Base directory for gold layer data
    
    Returns:
        training_data: Complete DataFrame with features, labels, and metadata
        feature_names: List of feature column names
        categorical_features: List of categorical feature indices for LightGBM
    """
    part_suffix = snapshot_date_str.replace('-', '_')
    
    # Load gold label store (cumulative)
    label_path = f"{gold_base_dir}label_store/gold_label_store_{part_suffix}.parquet"
    df_labels = pd.read_parquet(label_path)
    print(f"✅ Loaded labels: {len(df_labels)} customers")
    
    # Load ALL gold feature store files up to this date (cumulative approach)
    feature_files = []
    snapshot_datetime = pd.to_datetime(snapshot_date_str)
    
    # Find all feature files up to the snapshot date
    all_feature_files = glob.glob(f"{gold_base_dir}feature_store/gold_feature_store_*.parquet")
    
    for file_path in all_feature_files:
        # Extract date from filename
        filename = os.path.basename(file_path)
        date_part = filename.replace('gold_feature_store_', '').replace('.parquet', '')
        try:
            file_date = pd.to_datetime(date_part.replace('_', '-'))
            if file_date <= snapshot_datetime:
                feature_files.append((file_date, file_path))
        except:
            continue
    
    # Sort by date and load features
    feature_files.sort(key=lambda x: x[0])
    print(f"📁 Found {len(feature_files)} feature files up to {snapshot_date_str}")
    
    # Load and combine all feature files
    all_features = []
    for file_date, file_path in feature_files:
        df_features = pd.read_parquet(file_path)
        all_features.append(df_features)
    
    # Combine all features
    df_all_features = pd.concat(all_features, ignore_index=True)
    print(f"📊 Combined features: {len(df_all_features)} records")
    
    # For each customer in labels, get their most recent features
    df_all_features['snapshot_date'] = pd.to_datetime(df_all_features['snapshot_date'])
    latest_features = df_all_features.groupby('Customer_ID').last().reset_index()
    print(f"🔗 Latest features per customer: {len(latest_features)} customers")
    
    # Join with labels
    training_data = latest_features.merge(
        df_labels[['Customer_ID', 'target']], 
        on='Customer_ID', 
        how='inner'
    )
    
    print(f"🔗 Merged training data: {len(training_data)} customers")
    
    # Check target distribution
    target_dist = training_data['target'].value_counts().sort_index()
    print(f"🎯 Target distribution:")
    imbalance_ratio = target_dist.min() / target_dist.max() if len(target_dist) >= 2 else 1.0
    
    for target, count in target_dist.items():
        if pd.notna(target):
            print(f"   Target {int(target)}: {count} customers ({count/len(training_data):.1%})")
    
    print(f"⚖️  Class imbalance ratio: {imbalance_ratio:.3f}")
    
    # Identify feature types for LightGBM
    exclude_cols = ['Customer_ID', 'snapshot_date', 'target']
    feature_columns = [col for col in training_data.columns if col not in exclude_cols]
    
    X = training_data[feature_columns]
    numerical_features = X.select_dtypes(include=[np.number]).columns.tolist()
    categorical_features = X.select_dtypes(include=['object']).columns.tolist()
    
    print(f"📊 Feature types: {len(numerical_features)} numerical, {len(categorical_features)} categorical")
    
    # LightGBM-specific preprocessing
    # Only handle categorical features - LightGBM handles missing values automatically
    categorical_feature_indices = []
    
    if len(categorical_features) > 0:
        print(f"🔧 Processing categorical features for LightGBM...")
        for col in categorical_features:
            # Fill missing categorical values with a placeholder
            training_data[col] = training_data[col].fillna('Unknown')
            
            # LightGBM can handle categorical features directly
            # Convert to category type for better performance
            training_data[col] = training_data[col].astype('category')
            
            # Get the index of this categorical feature
            categorical_feature_indices.append(feature_columns.index(col))
            print(f"   Processed {col}: {training_data[col].nunique()} categories")
    
    # Note: We deliberately do NOT fill missing values in numerical features
    # LightGBM handles missing values natively and often performs better with them
    print(f"💡 Keeping missing values in numerical features - LightGBM handles them natively")
    
    print(f"📊 Final training data shape: {training_data[feature_columns].shape}")
    print(f"📋 Features: {len(feature_columns)}")
    print(f"📂 Categorical feature indices: {categorical_feature_indices}")
    
    return training_data, feature_columns, categorical_feature_indices


def temporal_train_test_split(training_data, feature_names, test_ratio=0.2):
    """
    Perform temporal split for time series data - same as Model 1
    """
    # Sort by snapshot_date to ensure temporal order
    training_data_sorted = training_data.sort_values('snapshot_date')
    
    # Get unique snapshot dates and calculate split point
    unique_dates = sorted(training_data_sorted['snapshot_date'].unique())
    n_dates = len(unique_dates)
    
    if n_dates < 3:
        print("⚠️  Warning: Insufficient temporal diversity for temporal split")
        print("   Falling back to stratified random split")
        
        X = training_data_sorted[feature_names]
        y = training_data_sorted['target']
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_ratio, random_state=42, stratify=y
        )
        
        split_info = {
            'split_type': 'random',
            'reason': 'insufficient_temporal_data',
            'train_samples': len(X_train),
            'test_samples': len(X_test)
        }
        
        return X_train, X_test, y_train, y_test, split_info
    
    # Calculate temporal split point
    split_idx = int(n_dates * (1 - test_ratio))
    split_date = unique_dates[split_idx]
    
    print(f"📅 Temporal split:")
    print(f"   Training period: {unique_dates[0]} to {unique_dates[split_idx-1]}")
    print(f"   Testing period: {split_date} to {unique_dates[-1]}")
    
    # Split data based on time
    train_mask = training_data_sorted['snapshot_date'] < split_date
    test_mask = training_data_sorted['snapshot_date'] >= split_date
    
    train_data = training_data_sorted[train_mask]
    test_data = training_data_sorted[test_mask]
    
    print(f"   Train samples: {len(train_data)}")
    print(f"   Test samples: {len(test_data)}")
    
    # Prepare features and targets
    X_train = train_data[feature_names]
    X_test = test_data[feature_names]
    y_train = train_data['target']
    y_test = test_data['target']
    
    # Check class distribution in both sets
    train_target_dist = y_train.value_counts().to_dict()
    test_target_dist = y_test.value_counts().to_dict()
    
    print(f"   Train target distribution: {train_target_dist}")
    print(f"   Test target distribution: {test_target_dist}")
    
    # Check if we have both classes in both sets
    if len(train_target_dist) < 2 or len(test_target_dist) < 2:
        print("⚠️  Warning: Insufficient class diversity in temporal split")
        print("   Falling back to stratified random split")
        
        X = training_data_sorted[feature_names]
        y = training_data_sorted['target']
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_ratio, random_state=42, stratify=y
        )
        
        split_info = {
            'split_type': 'random',
            'reason': 'insufficient_class_diversity',
            'train_samples': len(X_train),
            'test_samples': len(X_test)
        }
        
        return X_train, X_test, y_train, y_test, split_info
    
    split_info = {
        'split_type': 'temporal',
        'split_date': split_date,
        'train_period': f"{unique_dates[0]} to {unique_dates[split_idx-1]}",
        'test_period': f"{split_date} to {unique_dates[-1]}",
        'train_samples': len(X_train),
        'test_samples': len(X_test),
        'train_target_dist': train_target_dist,
        'test_target_dist': test_target_dist
    }
    
    return X_train, X_test, y_train, y_test, split_info


def handle_class_imbalance(X_train, y_train, imbalance_ratio, strategy='auto'):
    """
    Handle class imbalance using SMOTE or undersampling
    
    Args:
        X_train: Training features
        y_train: Training targets
        imbalance_ratio: Ratio of minority to majority class
        strategy: 'auto', 'smote', 'undersample', or 'none'
    
    Returns:
        X_resampled, y_resampled: Resampled training data
        resampling_info: Information about the resampling process
    """
    print(f"\n⚖️  Handling Class Imbalance (ratio: {imbalance_ratio:.3f})...")
    
    original_counts = y_train.value_counts().sort_index()
    print(f"   Original distribution: {original_counts.to_dict()}")
    
    # Determine strategy
    if strategy == 'auto':
        if imbalance_ratio < 0.1:
            strategy = 'smote'
        elif imbalance_ratio < 0.3:
            strategy = 'undersample'  
        else:
            strategy = 'none'
        print(f"   Auto-selected strategy: {strategy}")
    
    if strategy == 'none':
        print("   No resampling applied")
        return X_train, y_train, {'strategy': 'none', 'original_counts': original_counts.to_dict()}
    
    try:
        if strategy == 'smote':
            # Use SMOTE for oversampling
            smote = SMOTE(random_state=42, k_neighbors=min(5, original_counts.min()-1))
            X_resampled, y_resampled = smote.fit_resample(X_train, y_train)
            
        elif strategy == 'undersample':
            # Use random undersampling
            undersampler = RandomUnderSampler(random_state=42)
            X_resampled, y_resampled = undersampler.fit_resample(X_train, y_train)
        
        resampled_counts = pd.Series(y_resampled).value_counts().sort_index()
        print(f"   Resampled distribution: {resampled_counts.to_dict()}")
        
        resampling_info = {
            'strategy': strategy,
            'original_counts': original_counts.to_dict(),
            'resampled_counts': resampled_counts.to_dict(),
            'original_samples': len(X_train),
            'resampled_samples': len(X_resampled)
        }
        
        return X_resampled, y_resampled, resampling_info
        
    except Exception as e:
        print(f"   ⚠️  Resampling failed: {e}")
        print(f"   Falling back to original data")
        return X_train, y_train, {'strategy': 'failed', 'error': str(e)}


def train_lightgbm_model(training_data, feature_names, categorical_features):
    """
    Train LightGBM model with class imbalance handling
    
    Args:
        training_data: Complete training dataset
        feature_names: List of feature names
        categorical_features: List of categorical feature indices
    
    Returns:
        model: Trained LightGBM model
        metrics: Training metrics dictionary
    """
    print("\n🚀 Training LightGBM Model with Class Imbalance Handling...")
    
    # Use temporal split
    X_train, X_test, y_train, y_test, split_info = temporal_train_test_split(
        training_data, feature_names, test_ratio=0.3
    )
    
    # Calculate imbalance ratio
    train_counts = y_train.value_counts().sort_index()
    imbalance_ratio = train_counts.min() / train_counts.max() if len(train_counts) >= 2 else 1.0
    
    # Handle class imbalance
    X_train_balanced, y_train_balanced, resampling_info = handle_class_imbalance(
        X_train, y_train, imbalance_ratio, strategy='auto'
    )
    
    # LightGBM parameters optimized for imbalanced data
    lgb_params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'num_leaves': 31,
        'learning_rate': 0.1,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'min_child_samples': 20,
        'min_child_weight': 0.001,
        'reg_alpha': 0.1,
        'reg_lambda': 0.1,
        'random_state': 42,
        'n_jobs': -1,
        'verbosity': -1,
        "is_unbalance": True, 
        'force_col_wise': True
    }
    
    # Handle class imbalance in LightGBM parameters
    # if imbalance_ratio < 0.5:
    #     scale_pos_weight = (y_train_balanced == 0).sum() / (y_train_balanced == 1).sum()
    #     lgb_params['scale_pos_weight'] = scale_pos_weight
    #     print(f"   Applied scale_pos_weight: {scale_pos_weight:.2f}")

    
    # Initialize LightGBM model
    lgb_model = lgb.LGBMClassifier(
        n_estimators=100,  # Will be optimized during training
        **lgb_params
    )
    
    # Create LightGBM datasets for advanced training

    train_data = lgb.Dataset(
        X_train_balanced, 
        label=y_train_balanced,
        categorical_feature=categorical_features,
        free_raw_data=False
    )
    valid_data = lgb.Dataset(
        X_test,
        label=y_test,
        free_raw_data=False
    )

    # Train with early stopping
    model_lgb = lgb.train(
        lgb_params,
        train_data,
        valid_sets=[train_data, valid_data],
        valid_names=["train", "valid"],
        
        num_boost_round=1000,  # 
        callbacks=[
            lgb.early_stopping(stopping_rounds=500),
            lgb.log_evaluation(period=0)
        ]

    )

    
    print(f"✅ Training completed! Best iteration: {model_lgb.best_iteration}")
    
    # Cross-validation

    if split_info['split_type'] == 'temporal' and len(X_train_balanced) > 300:
        # Use TimeSeriesSplit for temporal data
        tscv = TimeSeriesSplit(n_splits=3)
        # For LightGBM, we need to use the sklearn interface for CV
        lgb_sklearn = lgb.LGBMClassifier(
            n_estimators=model_lgb.best_iteration,
            **lgb_params
        )
        cv_scores = cross_val_score(lgb_sklearn, X_train_balanced, y_train_balanced, 
                                  cv=tscv, scoring='roc_auc')
        cv_type = "Temporal"
    else:
        # Use regular CV for small datasets
        lgb_sklearn = lgb.LGBMClassifier(
            n_estimators=model_lgb.best_iteration,
            **lgb_params
        )
        cv_scores = cross_val_score(lgb_sklearn, X_train_balanced, y_train_balanced, 
                                  cv=5, scoring='roc_auc')
        cv_type = "Standard"
    
    print(f"📊 {cv_type} Cross-validation AUC: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
    # print("🔄 Performing cross-validation...")

    # # Ensure n_estimators is at least 10
    # best_iter = model_lgb.best_iteration
    # if best_iter is None or best_iter < 10:
    #     best_iter = 10
    #     print(f"⚠️  Warning: best_iteration={model_lgb.best_iteration}, using fallback n_estimators={best_iter}")

    # # Create LGBMClassifier with safe n_estimators
    # lgb_sklearn = lgb.LGBMClassifier(
    #     n_estimators=best_iter,
    #     **lgb_params
    # )

    # if split_info['split_type'] == 'temporal' and len(X_train_balanced) > 300:
    #     tscv = TimeSeriesSplit(n_splits=3)
    #     cv_scores = cross_val_score(lgb_sklearn, X_train_balanced, y_train_balanced, 
    #                                 cv=tscv, scoring='roc_auc')
    #     cv_type = "Temporal"
    # else:
    #     cv_scores = cross_val_score(lgb_sklearn, X_train_balanced, y_train_balanced, 
    #                                 cv=5, scoring='roc_auc')
    #     cv_type = "Standard"

    # print(f"📊 {cv_type} Cross-validation AUC: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

    # Predictions and metrics on original test set
    y_pred_proba = model_lgb.predict(X_test, num_iteration=model_lgb.best_iteration)
    y_pred = (y_pred_proba > 0.4).astype(int)
    
    # Calculate metrics
    auc_score = roc_auc_score(y_test, y_pred_proba)
    
    print(f"\n📈 Model Performance ({split_info['split_type'].title()} Split):")
    print(f"   AUC Score: {auc_score:.4f}")
    print(f"   CV AUC: {cv_scores.mean():.4f}")
    print(f"   Best Iteration: {model_lgb.best_iteration}")
    
    # Classification report
    print(f"\n📋 Classification Report:")
    print(classification_report(y_test, y_pred))
    
    # Feature importance analysis
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': model_lgb.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    print(f"\n🔍 Top 10 Important Features:")
    for idx, row in feature_importance.head(10).iterrows():
        print(f"   {row['feature']}: {row['importance']:.0f}")
    
    # Compile metrics
    metrics = {
        'auc_score': auc_score,
        'cv_auc_mean': cv_scores.mean(),
        'cv_auc_std': cv_scores.std(),
        'cv_type': cv_type,
        'best_iteration': int(model_lgb.best_iteration),
        'split_info': split_info,
        'resampling_info': resampling_info,
        'train_samples': len(X_train),
        'test_samples': len(X_test),
        'balanced_train_samples': len(X_train_balanced),
        'n_features': len(feature_names),
        'categorical_features': categorical_features,
        'lgb_params': lgb_params,
        'feature_importance': feature_importance.to_dict('records')
    }
    
    return model_lgb, metrics


def save_model_artifacts(model, metrics, feature_names, snapshot_date_str, model_bank_dir):
    """
    Save LightGBM model and related artifacts
    """
    print(f"\n💾 Saving LightGBM model artifacts...")
    
    # Create model bank directory
    os.makedirs(model_bank_dir, exist_ok=True)
    
    # Model filename with date - use LightGBM native format
    model_filename = f"lgb_model_{snapshot_date_str.replace('-', '_')}.txt"
    model_path = os.path.join(model_bank_dir, model_filename)
    
    # Save the LightGBM model in native format (faster loading)
    model.save_model(model_path)
    print(f"✅ LightGBM model saved: {model_path}")
    
    # Also save as pickle for sklearn compatibility
    pickle_filename = f"lgb_model_{snapshot_date_str.replace('-', '_')}.pkl"
    pickle_path = os.path.join(model_bank_dir, pickle_filename)
    joblib.dump(model, pickle_path)
    print(f"✅ Pickle model saved: {pickle_path}")
    
    # Save metrics
    metrics_filename = f"lgb_metrics_{snapshot_date_str.replace('-', '_')}.json"
    metrics_path = os.path.join(model_bank_dir, metrics_filename)
    
    # Add metadata to metrics
    metrics['model_type'] = 'LightGBM'
    metrics['training_date'] = snapshot_date_str
    metrics['model_filename'] = model_filename
    metrics['pickle_filename'] = pickle_filename
    metrics['feature_names'] = feature_names
    metrics['created_at'] = datetime.now().isoformat()
    
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"✅ Metrics saved: {metrics_path}")
    
    # Save feature importance plot
    plt.figure(figsize=(12, 8))
    feature_importance_df = pd.DataFrame(metrics['feature_importance'])
    top_features = feature_importance_df.head(20)  # Show more features for LightGBM
    
    plt.barh(range(len(top_features)), top_features['importance'])
    plt.yticks(range(len(top_features)), top_features['feature'])
    plt.xlabel('Feature Importance (Gain)')
    plt.title(f'LightGBM Feature Importance - {snapshot_date_str}')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    
    plot_filename = f"lgb_feature_importance_{snapshot_date_str.replace('-', '_')}.png"
    plot_path = os.path.join(model_bank_dir, plot_filename)
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✅ Feature importance plot saved: {plot_path}")


def process_model_2_automl(snapshot_date_str, gold_base_dir, model_bank_dir):
    """
    Main function to train LightGBM model
    
    Args:
        snapshot_date_str: Training date in YYYY-MM-DD format
        gold_base_dir: Base directory for gold layer data
        model_bank_dir: Directory to save trained models
    
    Returns:
        success: Boolean indicating if training was successful
    """
    print(f"\n🤖 Model 2 AutoML - LightGBM Training")
    print(f"📅 Training Date: {snapshot_date_str}")
    print("=" * 60)
    
    # Check training eligibility
    eligible, reason = check_training_eligibility(snapshot_date_str)
    print(f"🔍 Training eligibility check: {reason}")
    
    if not eligible:
        print(f"⏭️  Skipping training for {snapshot_date_str}")
        print(f"✅ Task completed (skipped)")
        return True
    
    try:
        # Load training data
        training_data, feature_names, categorical_features = load_training_data(
            snapshot_date_str, gold_base_dir
        )
        
        # Check if we have enough data
        if len(training_data) < 100:
            print(f"❌ Insufficient training data: {len(training_data)} samples")
            return False
        
        target_counts = training_data['target'].value_counts()
        if len(target_counts) < 2 or target_counts.min() < 10:
            print(f"❌ Insufficient samples in minority class: {target_counts.to_dict()}")
            return False
        
        # Train model
        model, metrics = train_lightgbm_model(training_data, feature_names, categorical_features)
        
        # Save artifacts
        save_model_artifacts(model, metrics, feature_names, snapshot_date_str, model_bank_dir)
        
        print(f"\n🎉 Model 2 (LightGBM) training completed successfully!")
        print(f"   AUC Score: {metrics['auc_score']:.4f}")
        print(f"   Best Iteration: {metrics['best_iteration']}")
        print(f"   Split Type: {metrics['split_info']['split_type'].title()}")
        print(f"   Resampling: {metrics['resampling_info']['strategy']}")
        print(f"   Samples: {len(training_data)}")
        print(f"   Features: {len(feature_names)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in LightGBM training: {str(e)}")
        raise e


def main():
    """
    Main function to handle command line arguments and execute training
    """
    parser = argparse.ArgumentParser(description='Train LightGBM model (Model 2) with class imbalance handling')
    parser.add_argument('--snapshotdate', required=True, help='Snapshot date in YYYY-MM-DD format')
    parser.add_argument('--gold_base_dir', 
                       default='datamart/gold/', 
                       help='Gold layer base directory')
    parser.add_argument('--model_bank_dir',
                       default='model_bank/',
                       help='Model bank directory')
    
    args = parser.parse_args()
    
    # Execute training
    success = process_model_2_automl(
        snapshot_date_str=args.snapshotdate,
        gold_base_dir=args.gold_base_dir,
        model_bank_dir=args.model_bank_dir
    )
    
    if success:
        print("\n✅ Model 2 AutoML completed successfully!")
    else:
        print("\n❌ Model 2 AutoML failed!")
        exit(1)


if __name__ == "__main__":
    main()