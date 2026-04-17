# BDE-Finals

Step 1: To start docker compose <br>
docker-compose up -d --build<br>

--verify all containers are up <br>
docker ps

Step 3: Install dashboard.py dependencies <br>

cd dashboard <br>
pip install -r requirements.txt

Step 4: Launch streamlit<br>
streamlit run dashboard.py<br>


----- To stop the pipeline<br>

docker-compose down<br>

then Ctrl + C on streamlit run<br>

Data will still be in MongoDB unless you used docker-compose down -v.
