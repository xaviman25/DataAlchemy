# Description

<td><p>Building an End-to-End ETL Pipeline</p> <ol><li>Create a local PostgreSQL database and fire the <a href="https://drive.google.com/file/d/1x-LjQQ2OPV5t_TtZGLFgpBLqytMymFCW/view?usp=drive_link" class="is-external-link">script</a>.</li> <li>Extract the records from the “tmp.employees_raw” table.</li> <li>Perform the following validation on the records. If any of the records do not satisfy adhere to the rules then those records need to be removed.<ol><li>Name - Must not be empty.</li> <li>Email - Must be a valid email format and unique.</li> <li>Salary - Convert to a numeric type and ensure it is positive.</li> <li>Department - No transformation required.</li> <li>Join Date - Convert to a DATE type in the format 'YYYY-MM-DD' and it should not be in the future.</li></ol></li> <li>Insert these records in the “tmp.employees_processed” table.</li></ol> <p>The focus here needs to be efficiency, idempotency, and modularity, i.e., we should be able to run each step in the pipeline independently. Write unit test cases using Pytest with at least 90% code coverage.</p></td>

---

# How to test?
### Steps to start the project locally :

- Create a virtual environment
`python -m venv .venv`

- Activate the virtual environment
- On Windows
`.venv/Scripts/activate`
- On macOS and Linux
`source .venv/bin/activate`


- Install requirements
`pip install -r requirements.txt`

### Run test
run `pytest --cov`

---
### Checklist for review

- [x] Is the testing checklist completed?
- [x] Is the documentation updated?
- [x] Is the code free of typos?
- [x] Is the code formatted?

---
# Output Screenshots:

### 1. Code output:
![Screenshot (19)](https://github.com/Everestek/etl-induction/assets/168069760/ede8da35-5eb3-4064-8dbf-e67f8975a9e7)

### 2. Test cases output
![Screenshot (18)](https://github.com/Everestek/etl-induction/assets/168069760/73898c36-fe8a-467e-adf4-f39937a9e92e)

### 3. Raw table preview
![Screenshot (20)](https://github.com/Everestek/etl-induction/assets/168069760/03b7988d-0e70-485c-bcdf-c78e8389218e)

### 4. Processed table preview
![Screenshot (21)](https://github.com/Everestek/etl-induction/assets/168069760/d81a1ade-c2f5-498f-98a5-ec4dc480b6fe)

### 5. Outliers table preview:
![Screenshot (22)](https://github.com/Everestek/etl-induction/assets/168069760/c7514092-070b-42ab-a704-ae3233f11a66)

---
## Steps to start the project locally :
- Create a virtual environment
`python -m venv .venv`

- Activate the virtual environment

- On Windows
`.venv/Scripts/activate`

- On macOS and Linux
`source .venv/bin/activate`

- Install requirements
`pip install -r requirements.txt`

- Run test
`python -m unittest test01.py`
