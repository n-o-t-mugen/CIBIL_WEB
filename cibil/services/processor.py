"""
CIBIL Document Processor - Integrated with Django
Handles immediate processing of uploaded CIBIL documents
"""
import os
import re
import json
import boto3
import psycopg2
import logging
import tempfile
from datetime import datetime
from django.conf import settings
from urllib.parse import urlparse

# Import the extractor (assuming it's in the same directory)
try:
    from .extractor import CIBILDataExtractor
    extractor = CIBILDataExtractor()
except ImportError:
    # Fallback for development
    extractor = None
    logging.warning("CIBILDataExtractor not found, using mock extractor")

class CIBILProcessor:
    """Processes CIBIL documents and updates database"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.s3_client = self._setup_s3_client()
        
    def _setup_logging(self):
        """Setup logging for processor"""
        logger = logging.getLogger('cibil_processor')
        logger.setLevel(logging.INFO)
        
        # Only add handlers if they don't exist
        if not logger.handlers:
            # Create log directory
            log_dir = os.path.join(settings.BASE_DIR, "logs", "processor")
            os.makedirs(log_dir, exist_ok=True)
            
            # File handler
            log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            
            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter('%(levelname)s: %(message)s')
            console_handler.setFormatter(console_formatter)
            
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger
    
    def _setup_s3_client(self):
        """Setup S3 client"""
        return boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
    
    def _get_file_extension(self, path: str) -> str:
        """Get file extension"""
        return os.path.splitext(path)[1].lower()
    
    def _sanitize_filename(self, s: str, max_len: int = 100) -> str:
        """Sanitize filename component"""
        s = (s or "").strip()
        s = re.sub(r"[^\w\s-]", "", s)
        s = re.sub(r"[-\s]+", "_", s).strip("_")
        return s[:max_len]
    
    def _build_s3_filename(self, name: str, pan_card: str, original_filename: str) -> str:
        """Build processed filename for S3"""
        ext = self._get_file_extension(original_filename)
        combined = f"{name}_{pan_card}" if pan_card else name
        safe = self._sanitize_filename(combined).upper()
        if not safe:
            safe = self._sanitize_filename(
                os.path.splitext(os.path.basename(original_filename))[0]
            ).upper()
        return f"{safe}{ext}"
    
    def _parse_report_date(self, report_date_str: str):
        """Parse report date string to datetime"""
        DATE_FORMATS = (
            "%d/%m/%Y, %H:%M",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d.%m.%Y",
        )
        DATE_PATTERNS = (
            r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})",
            r"(\d{4}[/-]\d{1,2}[/-]\d{1,2})",
        )
        
        if not report_date_str or not str(report_date_str).strip():
            now = datetime.now()
            dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
            return dt.strftime("%Y-%m-%d 00:00:00"), dt
        
        cleaned = str(report_date_str).strip()
        for fmt in DATE_FORMATS:
            try:
                dt = datetime.strptime(cleaned, fmt)
                dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                return dt.strftime("%Y-%m-%d 00:00:00"), dt
            except ValueError:
                continue
        
        for pattern in DATE_PATTERNS:
            match = re.search(pattern, cleaned)
            if match:
                try:
                    date_part = match.group(1).replace("-", "/")
                    dt = datetime.strptime(date_part, "%d/%m/%Y")
                    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                    return dt.strftime("%Y-%m-%d 00:00:00"), dt
                except ValueError:
                    continue
        
        now = datetime.now()
        dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return dt.strftime("%Y-%m-%d 00:00:00"), dt
    
    def _s3_key_from_url(self, file_url: str) -> str | None:
        """Extract S3 key from URL"""
        if not file_url:
            return None
        try:
            parsed = urlparse(file_url)
            return parsed.path.lstrip("/") or None
        except Exception:
            return None
    
    def _db_connect(self):
        """Connect to PostgreSQL database"""
        self.logger.debug("Connecting to PostgreSQL")
        return psycopg2.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            database=settings.DB_NAME,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
        )
    
    def _get_latest_record(self, conn, name: str, pan_card: str):
        """Get latest record by name and PAN"""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT report_data_and_time, url
                FROM public.table_cibil
                WHERE name = %s AND pan_card = %s
                ORDER BY report_data_and_time DESC
                LIMIT 1
                """,
                (name, pan_card),
            )
            row = cur.fetchone()
            if not row:
                return None
            return {"report_data_and_time": row[0], "url": row[1]}
    
    def _delete_all_records(self, conn, name: str, pan_card: str):
        """Delete all records by name and PAN"""
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM public.table_cibil
                WHERE name = %s AND pan_card = %s;
                """,
                (name, pan_card),
            )
    
    def _insert_record(self, conn, record: dict):
        """Insert record into database"""
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.table_cibil
                (name, mobile_no, pan_card, email, report_data_and_time, score, ckys, summary, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    record["name"],
                    record["mobile_no"],
                    record["pan_card"],
                    record["email"],
                    record["report_data_and_time"],
                    record["score"],
                    record["ckys"],
                    record["summary"],
                    record["url"],
                ),
            )
    
    def _delete_s3_file(self, file_url: str):
        """Delete file from S3"""
        key = self._s3_key_from_url(file_url)
        if not key:
            return
        try:
            self.s3_client.delete_object(Bucket=settings.AWS_S3_BUCKET_NAME, Key=key)
            self.logger.info(f"Deleted old S3 object: {key}")
        except Exception as e:
            self.logger.warning(f"S3 delete failed for {key}: {e}")
    
    def _upload_processed_file(self, local_file_path: str, s3_filename: str) -> str:
        """Upload processed file to processed-data/ folder"""
        s3_key = f"processed-data/{s3_filename}"
        self.logger.info(f"Uploading processed file to S3 key={s3_key}")
        self.s3_client.upload_file(local_file_path, settings.AWS_S3_BUCKET_NAME, s3_key)
        url = f"https://{settings.AWS_S3_BUCKET_NAME}.s3.{settings.AWS_REGION}.amazonaws.com/{s3_key}"
        self.logger.debug(f"S3 URL: {url}")
        return url
    
    def _build_db_record(self, cibil_data: dict, s3_url: str, parsed_date: str) -> dict:
        """Build database record from extracted data"""
        basic_info = cibil_data.get("basic_info") or {}
        overdue_summary = cibil_data.get("overdue_summary") or {}
        enquiries = cibil_data.get("enquiries") or {}
        accounts = cibil_data.get("accounts") or {}

        mobile_numbers = basic_info.get("mobile_numbers") or []
        emails = basic_info.get("emails") or []

        try:
            score = int(basic_info.get("score", 0))
        except Exception:
            score = 0
        
        summary_obj = {
            "score": score,
            "overdue_accounts": overdue_summary.get("total_overdue_accounts", 0),
            "overdue_amount": overdue_summary.get("total_overdue_amount", 0),
            "current_amount": overdue_summary.get("total_current_amount", 0),
            "enquiry_count": len(enquiries.get("latest_month_enquiries") or []),
            "default_months": accounts.get("final_default_month_average", 0),
        }

        return {
            "name": basic_info.get("name", "").strip().lower(),
            "pan_card": basic_info.get("pan_card", "").strip().upper(),
            "mobile_no": ",".join(dict.fromkeys(mobile_numbers)) if mobile_numbers else "",
            "email": ",".join(dict.fromkeys(emails)) if emails else "",
            "report_data_and_time": parsed_date,
            "score": score,
            "ckys": (basic_info.get("ckyc") or "").strip(),
            "summary": json.dumps(summary_obj),
            "url": s3_url,
        }
    
    def _should_process_file(self, conn, name: str, pan: str, incoming_dt: datetime):
        """Check if file should be processed based on date comparison"""
        existing = self._get_latest_record(conn, name, pan)
        
        if not existing:
            return "INSERT_NEW", None, "No existing record - will insert"
        
        existing_dt = existing["report_data_and_time"]
        
        # Normalize dates for comparison
        if isinstance(existing_dt, datetime):
            existing_date = existing_dt.date()
        else:
            try:
                existing_date = datetime.strptime(str(existing_dt)[:10], "%Y-%m-%d").date()
            except:
                existing_date = datetime.now().date()
        
        incoming_date = incoming_dt.date()
        
        if incoming_date > existing_date:
            reason = f"Incoming date {incoming_date} is newer than existing {existing_date}"
            return "REPLACE_OLD", existing, reason
        elif incoming_date == existing_date:
            reason = f"Incoming date {incoming_date} is same as existing {existing_date}"
            return "SKIP", existing, reason
        else:
            reason = f"Incoming date {incoming_date} is older than existing {existing_date}"
            return "SKIP", existing, reason
    
    def process_single_file(self, s3_key: str) -> dict:
        """
        Process a single file from S3 raw-data folder
        Returns: dict with processing status and details
        """
        result = {
            "status": None,
            "error_message": None,
            "reason": None,
            "name": None,
            "pan": None,
            "original_file": s3_key,
            "s3_url": None,
        }
        
        conn = None
        temp_path = None
        
        try:
            self.logger.info(f"Processing file: {s3_key}")
            
            # Download file to temp location
            original_filename = os.path.basename(s3_key)
            with tempfile.NamedTemporaryFile(
                suffix=os.path.splitext(original_filename)[1], 
                delete=False
            ) as tmp:
                temp_path = tmp.name
                self.s3_client.download_file(
                    settings.AWS_S3_BUCKET_NAME, 
                    s3_key, 
                    temp_path
                )
            
            # Extract data using extractor
            if extractor is None:
                raise ImportError("CIBILDataExtractor not available")
            
            cibil_data = extractor.extract(temp_path)
            basic_info = cibil_data.get("basic_info") or {}

            name = (basic_info.get("name") or "").strip().lower()
            pan = (basic_info.get("pan_card") or "").strip().upper()
            raw_report_date = basic_info.get("report_date")

            result["name"] = name
            result["pan"] = pan

            if not name or not pan:
                msg = "Missing name or PAN in extracted data"
                self.logger.error(f"{s3_key}: {msg}")
                result["status"] = "ERROR"
                result["error_message"] = msg
                return result

            # Parse report date
            parsed_str, parsed_dt = self._parse_report_date(raw_report_date)
            
            # Connect to database
            conn = self._db_connect()
            
            # Check if we should process this file
            decision, existing, reason = self._should_process_file(
                conn, name, pan, parsed_dt
            )
            result["reason"] = reason

            if decision == "SKIP":
                self.logger.info(f"{s3_key}: SKIPPED. {reason}")
                result["status"] = "SKIPPED"
                return result

            # Delete old records if replacing
            if decision == "REPLACE_OLD" and existing:
                self.logger.info(
                    f"{s3_key}: REPLACING older record. {reason}"
                )
                self._delete_s3_file(existing.get("url"))
                self._delete_all_records(conn, name, pan)

            # Build new filename and upload to processed-data/
            s3_filename = self._build_s3_filename(name, pan, original_filename)
            s3_url = self._upload_processed_file(temp_path, s3_filename)
            result["s3_url"] = s3_url

            # Build record and insert into database
            record = self._build_db_record(cibil_data, s3_url, parsed_str)
            self._insert_record(conn, record)
            
            # Commit transaction
            conn.commit()
            
            # Update result status
            result["status"] = "REPLACED" if decision == "REPLACE_OLD" else "INSERTED"
            self.logger.info(f"{s3_key}: {result['status']}")
            
            return result

        except Exception as e:
            # Rollback transaction on error
            if conn:
                conn.rollback()
            
            error_msg = f"Error processing file {s3_key}: {str(e)}"
            self.logger.error(error_msg)
            
            result["status"] = "ERROR"
            result["error_message"] = str(e)
            return result
            
        finally:
            # Clean up resources
            if conn:
                conn.close()
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except:
                    pass

# Create singleton instance
processor = CIBILProcessor()