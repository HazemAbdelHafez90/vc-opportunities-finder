from http.server import BaseHTTPRequestHandler
import os
import mimetypes

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/' or self.path == '':
            self.path = '/index.html'

        try:
            file_path = os.path.join(os.path.dirname(__file__), '..', self.path.lstrip('/'))

            if os.path.isfile(file_path):
                with open(file_path, 'rb') as f:
                    content = f.read()

                mime_type, _ = mimetypes.guess_type(file_path)
                if mime_type is None:
                    mime_type = 'application/octet-stream'

                self.send_response(200)
                self.send_header('Content-Type', mime_type)
                self.send_header('Cache-Control', 'max-age=3600')
                self.end_headers()
                self.wfile.write(content)
            else:
                self.send_response(404)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Not Found')
        except Exception as e:
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(f'Error: {str(e)}'.encode())
