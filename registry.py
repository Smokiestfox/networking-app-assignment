import json # đọc ds tài khoản
import os # thao tác hệ thống file
import threading #tạo khóa khi nhiều người cùng truy cập
from datetime import datetime # lấy ngày giờ hiện tại khi đăng nhập

class RegisterResult:
    def __init__(self, success: bool, message: str = ""):
        self.success = bool(success) 
        self.message = message

    def __bool__(self):
        return self.success

    def __iter__(self):
        return iter((self.success, self.message)) 

    def __getitem__(self, index):
        return (self.success, self.message)[index]

    def __eq__(self, other):
        if isinstance(other, bool):
            return self.success == other
        if isinstance(other, tuple):
            return (self.success, self.message) == other
        return super().__eq__(other)

     
class UserRegistry:
    """
    Quản lý danh sách tài khoản (xác thực) và người dùng đang hoạt động (active users).
    Đảm bảo an toàn đa luồng (thread-safe) với Lock.
    """
    def __init__(self, users_file="users.json"):
        self.users_file = users_file
        self.lock = threading.Lock()
        self.user_db = {}        # username -> password
        self.active_users = {}   # username -> {"username": ..., "ip": ..., "port": ..., "conn": ...}
        self.load_users()

    @property
    def _credentials(self):
        return self.user_db


    def load_users(self):
        """Đọc danh sách tài khoản từ file users.json nếu có."""
        if self.users_file and os.path.exists(self.users_file):
            try:
                with open(self.users_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        self.user_db = {k: str(v) for k, v in data.items()}
                        return
            except Exception as e:
                print(f"[UserRegistry] Lỗi đọc {self.users_file}: {e}")

    def authenticate(self, username: str, password: str = None) -> bool:
        """
        Kiểm tra thông tin đăng nhập của user.
        Trả về True nếu hợp lệ, False nếu sai mật khẩu hoặc user không tồn tại.
        """
        if not username or not isinstance(username, str):
            return False
        with self.lock:
            if username in self.user_db:
                expected_pass = self.user_db[username]
                if password is not None:
                    return str(password) == expected_pass
                return expected_pass == ""
            return False

    def register(self, username: str, ip: str, port: int, conn=None, sock=None):
        """
        Đăng ký user vào danh sách Active Users một cách ATOMIC.
        Lưu chính xác IP và active TCP port của client.
        Nếu user đã online (trùng username), từ chối đăng ký và trả về False.
        """
        connection = conn if conn is not None else sock
        
        with self.lock:
            if username in self.active_users:
                return RegisterResult(False, "ALREADY_LOGGED_IN")
            self.active_users[username] = {
                "username": username,
                "ip": str(ip),
                "port": int(port),
                "conn": connection,
                "login_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            return RegisterResult(True, "OK")

    def unregister(self, username: str = None, addr: tuple = None) -> bool:
        """
        Xóa user khỏi danh sách Active Users một cách an toàn.
        Có thể tìm theo username hoặc cặp (IP, Port).
        Trả về True nếu có user bị hủy đăng ký, ngược lại False.
        """
        with self.lock:
            target = None
            if username and username in self.active_users:
                target = username
            elif addr:
                ip, port = addr[0], addr[1]
                for u, info in self.active_users.items():
                    if info["ip"] == str(ip) and info["port"] == int(port):
                        target = u
                        break
            if target:
                del self.active_users[target]
                return True
            return False

    def is_active(self, username: str) -> bool:
        """Kiểm tra user có đang online không."""
        with self.lock:
            return username in self.active_users

    def get_user_list(self) -> list:
        """Trả về danh sách các user đang online kèm IP và Port."""
        with self.lock:
            return [
                {
                    "username": info["username"],
                    "ip": info["ip"],
                    "port": info["port"]
                }
                for info in self.active_users.values()
            ]

    def get_active_users(self) -> list:
        with self.lock:
            return list(self.active_users.keys())

    def format_active_users_table(self) -> str:
        """Tạo bảng biểu diễn Active Users (IP:Port) dạng ASCII table."""
        with self.lock:
            if not self.active_users:
                return (
                    "+----------------------+-----------------+--------------+\n"
                    "| Username             | IP Address      | Port         |\n"
                    "+----------------------+-----------------+--------------+\n"
                    "| (No active users)    | -               | -            |\n"
                    "+----------------------+-----------------+--------------+"
                )
            header = "+----------------------+-----------------+--------------+"
            title  = "| Username             | IP Address      | Port         |"
            rows = [header, title, header]
            for u, info in self.active_users.items():
                rows.append(f"| {u:<20} | {info['ip']:<15} | {info['port']:<12} |")
            rows.append(header)
            return "\n".join(rows)

    def get_user_session(self, username: str):
        with self.lock:
            info = self.active_users.get(username)
            if not info:
                return None
            class Session:
                username = info["username"]
                ip = info["ip"]
                tcp_port = info["port"]
            return Session()