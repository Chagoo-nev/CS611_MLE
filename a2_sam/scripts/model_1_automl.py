import argparse
import os
import pandas as pd
import numpy as np
import joblib
import json
import glob
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score, TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
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
    
    Args:
        snapshot_date_str: Date in YYYY-MM-DD format
        gold_base_dir: Base directory for gold layer data
    
    Returns:
        training_data: Complete DataFrame with features, labels, and metadata
        feature_names: List of feature column names
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
    for target, count in target_dist.items():
        if pd.notna(target):
            print(f"   Target {int(target)}: {count} customers ({count/len(training_data):.1%})")
    
    # Handle categorical and numerical features separately
    exclude_cols = ['Customer_ID', 'snapshot_date', 'target']
    feature_columns = [col for col in training_data.columns if col not in exclude_cols]
    
    X = training_data[feature_columns]
    numerical_features = X.select_dtypes(include=[np.number]).columns
    categorical_features = X.select_dtypes(include=['object']).columns
    
    print(f"📊 Feature types: {len(numerical_features)} numerical, {len(categorical_features)} categorical")
    
    # Handle missing values in numerical features
    if len(numerical_features) > 0:
        training_data[numerical_features] = training_data[numerical_features].fillna(
            training_data[numerical_features].median()
        )
    
    # Handle categorical features: Label encoding
    label_encoders = {}
    for col in categorical_features:
        le = LabelEncoder()
        # Handle missing values in categorical features
        training_data[col] = training_data[col].fillna('Unknown')
        training_data[col] = le.fit_transform(training_data[col])
        label_encoders[col] = le
        print(f"   Encoded {col}: {len(le.classes_)} categories")
    
    # Update feature names after encoding
    final_feature_columns = [col for col in training_data.columns if col not in exclude_cols]
    
    print(f"📊 Final training data shape: {training_data[final_feature_columns].shape}")
    print(f"📋 Features: {len(final_feature_columns)}")
    
    return training_data, final_feature_columns


def temporal_train_test_split(training_data, feature_names, test_ratio=0.2):
    """
    Perform temporal split for time series data
    
    Args:
        training_data: DataFrame with 'snapshot_date' column
        feature_names: List of feature column names
        test_ratio: Proportion of most recent data for testing
    
    Returns:
        X_train, X_test, y_train, y_test: Temporally split data
        split_info: Dictionary with split information
    """
    # For cumulative data, we need to split by customer acquisition time
    # Use the snapshot_date of features as a proxy for customer acquisition
    
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


def train_random_forest_model(training_data, feature_names):
    """
    Train Random Forest model with temporal cross-validation
    
    Args:
        training_data: Complete training dataset
        feature_names: List of feature names
    
    Returns:
        model: Trained RandomForest model
        metrics: Training metrics dictionary
    """
    print("\n🌲 Training Random Forest Model with Temporal Split...")
    
    # Use temporal split
    X_train, X_test, y_train, y_test, split_info = temporal_train_test_split(
        training_data, feature_names, test_ratio=0.2
    )
    
    # Initialize Random Forest model
    rf_model = RandomForestClassifier(
        n_estimators=100,           # Number of trees
        max_depth=10,               # Maximum depth to prevent overfitting
        min_samples_split=10,       # Minimum samples to split
        min_samples_leaf=5,         # Minimum samples in leaf
        max_features='sqrt',        # Features to consider for best split
        random_state=42,
        n_jobs=-1,                   # Use all available cores
        class_weight='balanced' 
    )
    
    # Train the model
    print("🔄 Training in progress...")
    rf_model.fit(X_train, y_train)
    print("✅ Training completed!")
    
    # Cross-validation based on split type
    print("🔄 Performing cross-validation...")
    
    if split_info['split_type'] == 'temporal' and len(X_train) > 300:
        # Use TimeSeriesSplit for temporal data
        tscv = TimeSeriesSplit(n_splits=3)
        cv_scores = cross_val_score(rf_model, X_train, y_train, cv=tscv, scoring='roc_auc')
        cv_type = "Temporal"
    else:
        # Use regular CV for small datasets or random splits
        cv_scores = cross_val_score(rf_model, X_train, y_train, cv=5, scoring='roc_auc')
        cv_type = "Standard"
    
    print(f"📊 {cv_type} Cross-validation AUC: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
    
    # Predictions and metrics
    y_pred = rf_model.predict(X_test)
    y_pred_proba = rf_model.predict_proba(X_test)[:, 1]
    
    # Calculate metrics
    auc_score = roc_auc_score(y_test, y_pred_proba)
    
    print(f"\n📈 Model Performance ({split_info['split_type'].title()} Split):")
    print(f"   AUC Score: {auc_score:.4f}")
    print(f"   CV AUC: {cv_scores.mean():.4f}")
    
    # Classification report
    print(f"\n📋 Classification Report:")
    print(classification_report(y_test, y_pred))
    
    # Feature importance analysis
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': rf_model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(f"\n🔍 Top 10 Important Features:")
    for idx, row in feature_importance.head(10).iterrows():
        print(f"   {row['feature']}: {row['importance']:.4f}")
    
    # Compile metrics
    metrics = {
        'auc_score': auc_score,
        'cv_auc_mean': cv_scores.mean(),
        'cv_auc_std': cv_scores.std(),
        'cv_type': cv_type,
        'split_info': split_info,
        'train_samples': len(X_train),
        'test_samples': len(X_test),
        'n_features': len(feature_names),
        'feature_importance': feature_importance.to_dict('records')
    }
    
    return rf_model, metrics


def save_model_artifacts(model, metrics, feature_names, snapshot_date_str, model_bank_dir):
    """
    Save model and related artifacts
    
    Args:
        model: Trained model
        metrics: Performance metrics
        feature_names: List of feature names
        snapshot_date_str: Training date
        model_bank_dir: Directory to save model artifacts
    """
    print(f"\n💾 Saving model artifacts...")
    
    # Create model bank directory
    os.makedirs(model_bank_dir, exist_ok=True)
    
    # Model filename with date
    model_filename = f"rf_model_{snapshot_date_str.replace('-', '_')}.pkl"
    model_path = os.path.join(model_bank_dir, model_filename)
    
    # Save the model
    joblib.dump(model, model_path)
    print(f"✅ Model saved: {model_path}")
    
    # Save metrics
    metrics_filename = f"rf_metrics_{snapshot_date_str.replace('-', '_')}.json"
    metrics_path = os.path.join(model_bank_dir, metrics_filename)
    
    # Add metadata to metrics
    metrics['model_type'] = 'RandomForest'
    metrics['training_date'] = snapshot_date_str
    metrics['model_filename'] = model_filename
    metrics['feature_names'] = feature_names
    metrics['created_at'] = datetime.now().isoformat()
    
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"✅ Metrics saved: {metrics_path}")
    
    # Save feature importance plot
    plt.figure(figsize=(10, 8))
    feature_importance_df = pd.DataFrame(metrics['feature_importance'])
    top_features = feature_importance_df.head(15)
    
    plt.barh(range(len(top_features)), top_features['importance'])
    plt.yticks(range(len(top_features)), top_features['feature'])
    plt.xlabel('Feature Importance')
    plt.title(f'Random Forest Feature Importance - {snapshot_date_str}')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    
    plot_filename = f"rf_feature_importance_{snapshot_date_str.replace('-', '_')}.png"
    plot_path = os.path.join(model_bank_dir, plot_filename)
    plt.savefig(plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✅ Feature importance plot saved: {plot_path}")


def process_model_1_automl(snapshot_date_str, gold_base_dir, model_bank_dir):
    """
    Main function to train Random Forest model
    
    Args:
        snapshot_date_str: Training date in YYYY-MM-DD format
        gold_base_dir: Base directory for gold layer data
        model_bank_dir: Directory to save trained models
    
    Returns:
        success: Boolean indicating if training was successful
    """
    print(f"\n🤖 Model 1 AutoML - Random Forest Training")
    print(f"📅 Training Date: {snapshot_date_str}")
    print("=" * 60)
    
    # Check training eligibility
    eligible, reason = check_training_eligibility(snapshot_date_str)
    print(f"🔍 Training eligibility check: {reason}")
    
    if not eligible:
        print(f"⏭️  Skipping training for {snapshot_date_str}")
        print(f"✅ Task completed (skipped)")
        return True  # Return True to indicate successful completion (skip is not a failure)
    
    try:
        # Load training data
        training_data, feature_names = load_training_data(snapshot_date_str, gold_base_dir)
        
        # Check if we have enough data
        if len(training_data) < 100:
            print(f"❌ Insufficient training data: {len(training_data)} samples")
            return False
        
        target_counts = training_data['target'].value_counts()
        if len(target_counts) < 2 or target_counts.min() < 10:
            print(f"❌ Insufficient samples in minority class: {target_counts.to_dict()}")
            return False
        
        # Train model
        model, metrics = train_random_forest_model(training_data, feature_names)
        
        # Save artifacts
        save_model_artifacts(model, metrics, feature_names, snapshot_date_str, model_bank_dir)
        
        print(f"\n🎉 Model 1 (Random Forest) training completed successfully!")
        print(f"   AUC Score: {metrics['auc_score']:.4f}")
        print(f"   Split Type: {metrics['split_info']['split_type'].title()}")
        print(f"   Samples: {len(training_data)}")
        print(f"   Features: {len(feature_names)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in model training: {str(e)}")
        raise e


def main():
    """
    Main function to handle command line arguments and execute training
    """
    parser = argparse.ArgumentParser(description='Train Random Forest model (Model 1) with temporal split')
    parser.add_argument('--snapshotdate', required=True, help='Snapshot date in YYYY-MM-DD format')
    parser.add_argument('--gold_base_dir', 
                       default='datamart/gold/', 
                       help='Gold layer base directory')
    parser.add_argument('--model_bank_dir',
                       default='model_bank/',
                       help='Model bank directory')
    
    args = parser.parse_args()
    
    # Execute training
    success = process_model_1_automl(
        snapshot_date_str=args.snapshotdate,
        gold_base_dir=args.gold_base_dir,
        model_bank_dir=args.model_bank_dir
    )
    
    if success:
        print("\n✅ Model 1 AutoML completed successfully!")
    else:
        print("\n❌ Model 1 AutoML failed!")
        exit(1)


if __name__ == "__main__":
    main()