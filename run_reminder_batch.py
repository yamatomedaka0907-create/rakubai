from app.db import init_db
from app.main import _process_reservation_reminders


if __name__ == "__main__":
    init_db()
    _process_reservation_reminders()
