import json # đọc ds tài khoản
import time #ghi lại tg đăng nhập
import threading #tạo khóa khi nhiều người cùng truy cập
from dataclasses import dataclass
from typing import Optional, Dict, List, Any

@dataclass
class UserSession: #lưu in4 1 client đang onl
    username: str
    ip: str
    tcp_port: int
    login_time: float
    sock: Any  #đối tượng socket TCP kết nối với client này

class UserRegistry:
    def __init__(self, users_file: str = "users.json"):
        self.users_file = users_file

        self._lock = threading.Lock() # khóa mỗi khi thêm xóa user 

        self._active_users: Dict[str, UserSession] = {} # lưu những ai đang onl 

        self._credentials: Dict[str, str] = self._load_credentials() # tự mở file users.json mỗi khi đăng nhập

    def _load_credentials(self) -> Dict[str, str]:
        try:
            with open(self.users_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def authenticate(self, username: str, password: str) -> bool:
        if username not in self._credentials: #kiểm tra username có trong ds ko
            return False
        return self._credentials[username] == password

    def register(self, username: str, ip: str, tcp_port: int, sock: Any) -> tuple: #Ktra trùng và thêm mới atomic trong cùng 1 lần giữ threading.Lock()
        with self._lock:
            if username in self._active_users:
                return False, "ALREADY_LOGGED_IN"

            session = UserSession(
                username = username,
                ip = ip,
                tcp_port = tcp_port,
                login_time = time.time(),
                sock = sock
            )
            self._active_users[username] = session
            return True, "OK"

    def unregister(self, username: str) -> bool: #khi thoát hoặc mất kết nối, xóa user khỏi danh sách onl
        with self._lock:
            if username in self._active_users:
                del self._active_users[username]
                return True
            return False
    
    def get_active_users(self) -> List[str]:
        with self._lock:
            return list(self._active_users.keys())

    def get_user_session(self, username: str) -> Optional[UserSession]:
        with self._lock:
            return self._active_users.get(username)