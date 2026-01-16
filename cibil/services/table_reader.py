"""
NOTE:
This service intentionally bypasses Django ORM and connects directly
to PostgreSQL for read-only access to an external reporting table.
This is for performance and separation of concerns.
"""

import psycopg2
from django.conf import settings


def get_cibil_records(search=None):
    conn = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )

    try:
        with conn.cursor() as cur:
            base_query = """
                SELECT
                    name,
                    pan_card,
                    mobile_no,
                    email,
                    score,
                    report_data_and_time::timestamp as report_date,
                    summary,
                    url
                FROM public.table_cibil
            """

            if search:
                query = base_query + """
                    WHERE
                        name ILIKE %s
                        OR pan_card ILIKE %s
                        OR mobile_no ILIKE %s
                        OR email ILIKE %s
                    ORDER BY report_data_and_time DESC
                """
                pattern = f"%{search}%"
                cur.execute(query, (pattern, pattern, pattern, pattern))

            else:
                query = base_query + """
                    ORDER BY RANDOM()
                    LIMIT 5
                """
                cur.execute(query)

            rows = cur.fetchall()

            return [
                {
                    "name": row[0] or None,
                    "pan": row[1] or None,
                    "mobile": row[2] or None,
                    "email": row[3] or None,
                    "score": row[4] or None,
                    "report_date": row[5],
                    "summary": row[6] or None,
                    "url": row[7] or None,
                }
                for row in rows
            ]

    finally:
        conn.close()
