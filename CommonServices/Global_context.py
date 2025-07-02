from threading import Lock
import threading


class GlobalContext:
       
    # def __new__(cls):
    #     if cls._instance is None:
    #         cls._instance = super(GlobalContext, cls).__new__(cls)
    #     return cls._instance

    # def set_value(self, key, value):
    #     self._global_dict[key] = value

    # def get_value(self, key):
    #     return self._global_dict.get(key, "")

    # def delete_value(self, key):
    #     if key in self._global_dict:
    #         del self._global_dict[key]

    # def clear_all(self):
    #     self._global_dict.clear()
        
    # def __new__(cls):
    #     # Singleton pattern with thread safety
    #     if not cls._instance:
    #         with cls._lock:
    #             if not cls._instance:
    #                 cls._instance = super(GlobalContext, cls).__new__(cls)
    #     return cls._instance
    
    
    
    _instance = None
    _lock = threading.Lock()  # Lock for thread safety
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:  # Double-checked locking
                    cls._instance = super(GlobalContext, cls).__new__(cls, *args, **kwargs)
                    cls._instance._global_dict = {}
                    cls._instance._is_live_running = False
        return cls._instance

    def set_value(self, key, value):
        # Thread-safe setting of values
        with self._lock:
            self._global_dict[key] = value

    def get_value(self, key):
        # Thread-safe retrieval of values
        with self._lock:
            return self._global_dict.get(key, "")

    def delete_value(self, key):
        # Thread-safe deletion of a value
        with self._lock:
            if key in self._global_dict:
                del self._global_dict[key]
    
    def clear_all(self):
        # Thread-safe clearing of the dictionary
        with self._lock:
            self._global_dict.clear()

    def get_all(self):
        # Returns a copy of the global dictionary
        with self._lock:
            return self._global_dict.copy()
        
        
    def set_live_running(self, is_running: bool):
        """Set the LIVE-SWITCH flag."""
        with self._lock:
         self._is_live_running = is_running

    def is_live_running(self):
        """Check if LIVE-SWITCH is in progress."""
        with self._lock:
         return self._is_live_running