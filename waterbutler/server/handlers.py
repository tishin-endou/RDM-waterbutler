import tornado.web

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core.models import http

from waterbutler.version import __version__


class StatusHandler(tornado.web.RequestHandler):

    def prepare(self):
        self.segment = xray_recorder.begin_segment('files.perfin.rdm.nii.ac.jp')
        self.segment.put_http_meta(http.URL, self.request.full_url())
        self.segment.put_http_meta(http.METHOD, self.request.method)
        super().prepare()

    def on_finish(self):
        if hasattr(self, 'segment'):
            self.segment.put_http_meta(http.STATUS, self.get_status())
            xray_recorder.end_segment()
        super().on_finish()

    def get(self):
        """List information about waterbutler status"""
        self.write({
            'status': 'up',
            'version': __version__
        })