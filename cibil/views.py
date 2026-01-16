from django.shortcuts import render, redirect
from django.contrib import messages
from django.utils import timezone
from .forms import DumpUploadForm
from .services.dumper import dumper
from django.conf import settings
from .services.table_reader import get_cibil_records


def dump_page(request):
    """Handle CIBIL document upload page - ONLY upload functionality"""
    
    # Check current time for display
    current_time = timezone.localtime()
    is_within_window = dumper.is_within_dump_window()

    context = {
        'form': DumpUploadForm(),
        'current_time': current_time,
        'dump_start': f"{settings.CIBIL_DUMP_START_HOUR:02d}:00",
        'dump_end': f"{settings.CIBIL_DUMP_END_HOUR:02d}:00",
        'is_within_window': is_within_window,
        'window_message': None,
        'now': timezone.now(),
    }

    if not is_within_window:
        context['window_message'] = (
            f"⚠️ Uploads are only accepted between {context['dump_start']} - {context['dump_end']}. "
            f"Current time: {current_time.strftime('%H:%M')}"
        )
    
    if request.method == 'POST':
        if not is_within_window:
            messages.error(request, 
                f"Uploads are only accepted between {context['dump_start']} - {context['dump_end']}")
            return render(request, 'cibil/dump.html', context)
        
        form = DumpUploadForm(request.POST, request.FILES)
        
        if form.is_valid():
            files = request.FILES.getlist('files')
            success, message = dumper.dump_files(files)

            if success:
                messages.success(request, message)
            else:
                messages.error(request, message)

            return redirect('cibil:cibil_dump')

        else:
            for error in form.errors.get('files', []):
                messages.error(request, error)
    
    return render(request, 'cibil/dump.html', context)


def search_page(request):
    """Handle CIBIL records search and display - ONLY search functionality"""
    search_query = request.GET.get("q", "").strip()
    records = get_cibil_records(search=search_query)

    context = {
        'records': records,
        'search_query': search_query,
        'now': timezone.now(),
    }

    return render(request, 'cibil/search.html', context)