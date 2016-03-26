from __future__ import unicode_literals

from django.db import models


# Create your models here.
class FrontPageUpload(models.Model):
    file = models.FileField(upload_to='frontPage/static/file_uploaded')
