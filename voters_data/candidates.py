import psycopg2
import requests
import random
import simplejson as json
from confluent_kafka import SerializingProducer
from main import generate_candidate_data, deliver_report

BASE_URL = "https://randomuser.me/api/?nat=gb"
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]

random.seed(42)
if __name__ == "__main__":


    producer = SerializingProducer(
        {
            "bootstrap.servers": "localhost:9092",
        }
    )

    try:
      conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
      cur = conn.cursor()

      
      cur.execute(
         """
           select * from candidate_name
         """
      )

      candidates = cur.fetchall()

      for i in range(1000):
            
        candidate = generate_candidate_data(i, 3) 
        cur.execute("""
                        INSERT INTO candidate_name (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['biography'],
                candidate['campaign_platform'], candidate['photo_url']))
        conn.commit()

        producer.produce(
                topic = "candidate_topic",
                key = candidate["candidate_id"],
                value = json.dumps(candidate),
                on_delivery= deliver_report

            )    

        print("Produced voter {} data: {}".format(i, candidate))
        producer.flush()


    except Exception as e:
        print(e)