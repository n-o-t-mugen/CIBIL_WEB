import os
import boto3
import logging
from datetime import datetime
from django.conf import settings
from botocore.exceptions import ClientError
from .processor import processor  

class CIBILDumper:
    """Handles dumping of CIBIL documents to S3"""
    
    def __init__(self):
        self.bucket_name = settings.AWS_S3_BUCKET_NAME
        self.raw_prefix = "raw-data/"
        self.region = settings.AWS_REGION
        
        # Setup logging
        self._setup_logging()
        
        # Setup S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=self.region
        )
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_dir = os.path.join(settings.BASE_DIR, "logs", "dumper")
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")
        
        self.logger = logging.getLogger('cibil_dumper')
        self.logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        if not self.logger.handlers:
            # File handler
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(file_format)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_format = logging.Formatter('%(levelname)s: %(message)s')
            console_handler.setFormatter(console_format)
            
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
    
    def is_within_dump_window(self):
        """Check if current time is within dump window"""
        now = datetime.now()
        current_hour = now.hour
        current_minute = now.minute
        
        start_hour = settings.CIBIL_DUMP_START_HOUR
        end_hour = settings.CIBIL_DUMP_END_HOUR
        
        self.logger.info(f"Checking dump window: Current time = {now.strftime('%H:%M')}")
        self.logger.info(f"Dump window: {start_hour:02d}:00 to {end_hour:02d}:00")
        
        if start_hour <= current_hour < end_hour:
            self.logger.info("Within dump window")
            return True
        
        # Handle edge case where current_hour == end_hour but minutes = 0
        if current_hour == end_hour and current_minute == 0:
            self.logger.info("Exactly at end hour (minute 00), allowing upload")
            return True
        
        self.logger.info("Outside dump window")
        return False
    
    def upload_file_to_s3(self, file_obj, filename):
        """Upload a single file to S3"""
        s3_key = self.raw_prefix + filename
        
        try:
            self.s3_client.upload_fileobj(
                file_obj,
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': self._get_content_type(filename),
                    'Metadata': {
                        'upload_time': datetime.now().isoformat(),
                        'original_filename': filename
                    }
                }
            )
            self.logger.info(f"Successfully uploaded: {filename} -> s3://{self.bucket_name}/{s3_key}")
            return True, s3_key
        except ClientError as e:
            self.logger.error(f"S3 upload failed for {filename}: {str(e)}")
            return False, str(e)
        except Exception as e:
            self.logger.error(f"Unexpected error uploading {filename}: {str(e)}")
            return False, str(e)
    
    def _get_content_type(self, filename):
        """Get content type based on file extension"""
        if filename.lower().endswith('.pdf'):
            return 'application/pdf'
        elif filename.lower().endswith('.html'):
            return 'text/html'
        else:
            return 'application/octet-stream'
    
    def dump_files(self, files):
        """Main method to dump multiple files to S3"""
        if not self.is_within_dump_window():
            message = f"Uploads only accepted between {settings.CIBIL_DUMP_START_HOUR:02d}:00 - {settings.CIBIL_DUMP_END_HOUR:02d}:00"
            self.logger.warning(message)
            return False, message
        
        if not files:
            self.logger.warning("No files provided for upload")
            return False, "No files provided"
        
        self.logger.info(f"Starting upload of {len(files)} file(s)")
        
        successful_uploads = []
        failed_uploads = []
        processed_details = {
            'inserted': 0,
            'replaced': 0,
            'skipped': 0,
            'errors': 0
        }
        
        for file_obj in files:
            filename = file_obj.name
            
            # Reset file pointer to beginning
            if hasattr(file_obj, 'seek'):
                file_obj.seek(0)
            
            # Upload to S3 raw-data/
            success, result = self.upload_file_to_s3(file_obj, filename)  # Changed variable name
            
            if success:
                s3_key = result  # Result contains s3_key on success
                # 🔥 IMMEDIATELY PROCESS THE FILE
                self.logger.info(f"Starting immediate processing for: {s3_key}")
                
                # Process the uploaded file
                processing_result = processor.process_single_file(s3_key)
                status = processing_result.get("status")
                
                if status in ["INSERTED", "REPLACED"]:
                    successful_uploads.append(filename)
                    
                    # Track processing details
                    if status == "INSERTED":
                        processed_details['inserted'] += 1
                        self.logger.info(f"Successfully inserted {filename}")
                    else:  # REPLACED
                        processed_details['replaced'] += 1
                        self.logger.info(f"Successfully replaced {filename}")
                        
                elif status == "SKIPPED":
                    successful_uploads.append(filename)  # Still counts as successful upload
                    processed_details['skipped'] += 1
                    self.logger.info(
                        f"File skipped (not processed): {filename} - {processing_result.get('reason')}"
                    )
                else:
                    # Processing failed
                    processed_details['errors'] += 1
                    failed_uploads.append((
                        filename, 
                        f"Upload succeeded but processing failed: {processing_result.get('error_message')}"
                    ))
                    self.logger.error(
                        f"Processing failed for {filename}: {processing_result.get('error_message')}"
                    )
            else:
                # Upload failed
                failed_uploads.append((filename, result))  # Result contains error message
        
        # Generate detailed summary message
        total_success = len(successful_uploads)
        total_failed = len(failed_uploads)
        
        self.logger.info(f"Upload complete. Successful: {total_success}, Failed: {total_failed}")
        self.logger.info(f"Processing details: Inserted={processed_details['inserted']}, "
                        f"Replaced={processed_details['replaced']}, "
                        f"Skipped={processed_details['skipped']}, "
                        f"Errors={processed_details['errors']}")
        
        if successful_uploads:
            # Build detailed message
            details_parts = []
            if processed_details['inserted'] > 0:
                details_parts.append(f"{processed_details['inserted']} inserted")
            if processed_details['replaced'] > 0:
                details_parts.append(f"{processed_details['replaced']} replaced")
            if processed_details['skipped'] > 0:
                details_parts.append(f"{processed_details['skipped']} skipped")
            
            details = f" ({', '.join(details_parts)})" if details_parts else ""
            
            message = f"Successfully uploaded {total_success} file(s){details}: {', '.join(successful_uploads)}"
            if failed_uploads:
                message += f". Failed: {total_failed} file(s)"
            return True, message
        else:
            message = f"Failed to upload all files. Errors: {', '.join([f[0] for f in failed_uploads])}"
            return False, message


# Singleton instance for easy import
dumper = CIBILDumper()