from django import forms
from django.core.exceptions import ValidationError
from django.conf import settings
import os

class MultiFileInput(forms.FileInput):
    allow_multiple_selected = True


class DumpUploadForm(forms.Form):
    # 🚨 NOT FileField
    files = forms.Field(
        widget=MultiFileInput(attrs={
            'accept': '.pdf,.html',
            'class': 'file-input'
        }),
        label='Select PDF/HTML files'
    )

    def clean(self):
        """
        Validate uploaded files using request.FILES
        """
        cleaned_data = super().clean()

        # IMPORTANT: access self.files, not cleaned_data
        files = self.files.getlist('files')

        if not files:
            raise ValidationError("Please select at least one file")

        for f in files:
            if f.size > settings.MAX_UPLOAD_SIZE:
                raise ValidationError(
                    f"File {f.name} exceeds maximum size (10MB)"
                )

            ext = os.path.splitext(f.name)[1].lower()
            if ext not in settings.ALLOWED_EXTENSIONS:
                raise ValidationError(
                    f"File {f.name} has unsupported extension"
                )

        return cleaned_data
