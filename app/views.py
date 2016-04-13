import json
import os

from django.http import HttpResponse
from django.shortcuts import render

# Create your views here.
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response

from appserver.dataAnalysis import record_constructor
from parkingPrediction import settings


@api_view(['GET'])
def home(request):
    if request.method == 'GET':
        return render(request, 'base.html')


@api_view(['POST'])
def upload(request):
    # if not request.user.is_authenticated():
    # json = simplejson.dumps({
    # 'success': False,
    # 'errors': {'__all__': 'Authentication required'}})
    # return HttpResponse(json, mimetype='application/json')
    # form = FrontPageUpload(request.POST or None, request.FILES or None)
    # if form.is_valid():
    # file = form.save()
    # json = serializers.serialize("json", FrontPageUpload.object.all())

    # else:
    # json = simplejson.dumps({'success': False, 'errors': form.errors})
    # return HttpResponse(json, content='application/json')

    # def ajax_get_data(self, request, format = None):
    # json_data = serializers.serialize("json", FrontPageUpload.object.all())
    # return HttpResponse(json_data, content_type = "application/json")
    if request.method == "POST":
        uploadedfile = request.FILES.get("file", None)
        if uploadedfile:
            localfile = os.path.join(settings.BASE_DIR + "/fileUploaded", uploadedfile.name)
            try:
                destination = open(localfile, 'wb+')
                for chunk in uploadedfile.chunks():
                    destination.write(chunk)
            except IOError:
                print 'cannot open', uploadedfile.name
                return Response(status=status.HTTP_400_BAD_REQUEST)
            else:
                destination.close()
                record_constructor(localfile)

            # analyze and computation
            data = open(os.path.join(settings.BASE_DIR) + '/app/sampleOutput.json').read()
            return HttpResponse(data)
    return Response(status=status.HTTP_400_BAD_REQUEST)
