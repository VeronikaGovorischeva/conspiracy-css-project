# Behavioural and Communication Patterns in Reddit Conspiracy Communities (2020)

## Short Research Summary  
This study analyzes Reddit conspiracy communities in 2020 and shows that discussion was dominated by COVID-19 and political events.  
Activity spiked during major real-world crises, and different subreddits reacted in a synchronized way.  
Communication was highly uniform: users mostly asked questions, expressed doubt, and tried to reinterpret uncertain events.  
Emotional tone was moderately negative, with fear being the most common emotion.  
Although most users posted only once, a small active minority shaped the overall discourse.  
Overall, the findings show that conspiracy communities function as a reactive ecosystem driven by uncertainty and major societal events.

---

## How to Run the Project  

### 1) `filter_file.py` — Filtering Raw Reddit Data  
This script filters large Pushshift Reddit archives downloaded as `.zst` files (e.g., `RC_2020-12.zst`).  
It extracts only posts/comments from the subreddits and dates you specify.

Before running, **you must update the script**:

- Change the **file names** of the Reddit `.zst` archives you want to filter.  
- Update the **path** to the folder where these archives are stored on your device.  
- Change the **output CSV file name and its path**.  
- Optionally adjust the **subreddit list** and **date filters** to customize the data you want.

The script will produce CSV files containing only the filtered Reddit data.

---

### 2) `normalizing.py` — Cleaning and Standardizing the Filtered Data

This script processes all filtered CSVs and outputs a single clean dataset.

#### What it does:
- Converts timestamps into a unified `date` column  
- Extracts titles and reconstructs proper Reddit URLs  
- Identifies subreddit names from permalinks  
- Cleans missing fields (title, body, author)  
- Labels each entry as **submission** or **comment**  
- Merges submissions and comments into one dataset  
- Removes duplicate posts/comments  
- Sorts all rows by date  
- Saves the final dataset to a CSV file  

#### Before running, you must edit:
- `submissions_folder` — path to your folder with filtered submission CSVs  
- `comments_folder` — path to your folder with filtered comment CSVs  
- `output_file` — name and path for the final dataset output  

This will produce a fully normalized dataset such as **normalizedFINALdata.csv**.

---

### 3) `eda.ipynb` — Exploratory Data Analysis

After filtering and normalizing data, you will obtain a dataset such as `dataset.csv`
containing only the subreddits and time ranges you selected earlier.

`eda.ipynb` performs the full exploratory data analysis.

#### To run:
1. Open the notebook in **Jupyter Notebook**, **JupyterLab**, or **VS Code**.  
2. Update the dataset path at the top of the notebook to the location of your CSV.  
3. Follow the instructions and comments inside the notebook step-by-step.

This notebook generates visualisations and statistical insights into conspiracy community behaviour.

---

### 4) `csv/` Folder — Additional Resources

The `csv/` folder contains supplementary CSV files used for easier visualization in the EDA notebook.  
They include preprocessed tables, external dataset links, or helper files used during analysis.

---

### Initial Dataset 

**[https://drive.google.com/file/d/1MGeq5bn4uhuSF6srA1FR5bcLGr6smbYF/view?usp=sharing]**

---

### File and Folder Structure

```
conspiracy-css-project/
│
├── filter_file.py            # Filters raw Reddit .zst archives into CSV subsets
├── normalizing.py            # Cleans & standardizes filtered CSV files
├── eda.ipynb                 # Final exploratory analysis notebook
│
├── csv/                      # Additional datasets used for visualizations
│   ├── covid_cooccurence.txt
│   └── sentiment.txt
│
└── README.md                 # Project documentation
```
