"""parkingPrediction URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.9/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url, include
from django.conf.urls.static import static
from django.contrib import admin

import frontPage.views
import settings

admin.autodiscover()

urlpatterns = [
                  url(r'^admin/', admin.site.urls),
                  url(r'^$', frontPage.views.home, name='home'),
                  url(r'^fileUpload', frontPage.views.upload, name='upload'),
                  url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework'))
              ] + static(settings.STATIC_URL)
