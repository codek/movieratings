# Exercise
Download the file and extract to /tmp
virtualenv venv
source venv/bin/activate
pip3 install -r requirements.txt
python3 functions/process_movies_ratings/app.py

# Future improvements
1. Get the file and unzip
2. Parameterise paths etc
3. Tests

# Alternative approach

An easier approach may be to do this as ELT
1. Dump the files to S3 in a versioned folder
2. Create a table definition for each raw file
3. Create an empty results table pointing at a new folder, stored as parquet
4. Create an Insert into X select MovieId, avg(rating) from Ratings group by MovieId etc
5. Potentially run tests, then upgrade the "final".

The double seperator may be an issue - if so; the line can be read in a single column and processed that way. Or the file can be pre-processed.
