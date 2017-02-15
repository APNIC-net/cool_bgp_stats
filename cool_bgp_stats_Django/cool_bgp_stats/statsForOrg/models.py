from __future__ import unicode_literals

from django.db import models


class StatFile(models.Model):
    stat_type = models.CharField(max_length=20)
    source_file = models.CharField(max_length=200)
    output_file = models.CharField(max_length=200)
    year = models.IntegerField()
    creation_date = models.DateTimeField('date created')
    
    def __str__(self):
        return self.output_file