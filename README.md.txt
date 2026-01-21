# Kaggle → Polars → MySQL Pipeline

This project automates the workflow of downloading a Kaggle dataset, processing it with Polars, and inserting it into MySQL.

---

## Folder Structure

kaggle_polars_mysql_pipeline/
│
├── data/
│   ├── raw/                 # Downloaded raw Kaggle datasets
│   └── processed/           # Cleaned / processed CSVs ready for MySQL
│
├── scripts/                 # Python scripts
│   ├── extract_kaggle.py    # Download dataset from Kaggle And processes data and than saves to csv 
│   └── load_mysql.py        # Insert processed CSV into MySQL
│
├── README.md                # Project explanation & instructions
└── requirements.txt         # Python dependencies
