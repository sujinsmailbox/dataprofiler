from types import SimpleNamespace
import json


class Config:
    def __init__(self, file_path):
        self.file_path = file_path
        self.config = None

    def parse_all_data(self):
        return json.loads(json.dumps(self.config), object_hook=lambda d: SimpleNamespace(**d))

    # Returns True if changed else False
    def reload(self):
        file_content = self.get_object_content()
        new_content = json.loads(file_content)

        if self.config == new_content:
            return False
        else:
            self.config = new_content
            # print("Config file contents are: {}".format(file_content))
            # print("Config has changed")
            return True

    def get_object_content(self):
        with open(self.file_path, "r") as f:
            return f.read()
