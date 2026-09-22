import os
import webbrowser
import app

# Render public web server settings
app.HOST = "0.0.0.0"
app.PORT = int(os.environ["PORT"])

# Prevent the server from trying to open a local browser
webbrowser.open = lambda *args, **kwargs: True
app.webbrowser.open = lambda *args, **kwargs: True

# Keep Render on the assigned port
app.find_free_port = lambda start_port, attempts=20: int(os.environ["PORT"])

import telegram_bot

telegram_bot.main()
