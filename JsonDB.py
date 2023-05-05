import json
import os
import msvcrt


class JsonDB:
    def __init__(self, path: str = None):
        base_path = str(os.path.dirname(os.path.abspath(__file__))) + '\\dbs'
        self.__Path = path if path else base_path
        self.__Database = None
        self.__Data: dict = {}
        self.__Config = self.__load_config()
        try:
            if self.__Path[1] == ':':
                os.mkdir(self.__Path)
            else:
                os.mkdir(f'{os.path.dirname(os.path.abspath(__file__))}\\{self.__Path}')
        except FileExistsError:
            pass

    def __getitem__(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_database_selected()
        key = str(key)
        try:
            return self.__Data[key]
        except KeyError:
            self.__Data[key] = {}
            return self.__Data[key]
        except TypeError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Database}" database') from None

    def __setitem__(self, key: str | int | float, value):
        self.__validate_key_type(key)
        self.__validate_database_selected()
        key = str(key)
        try:
            self.__Data[key] = value
        except KeyError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Database}" database') from None
        except TypeError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Database}" database') from None

    def __delitem__(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_database_selected()
        try:
            del self.__Data[key]
        except KeyError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Database}" database') from None

    def __iter__(self):
        self.__validate_database_selected()
        for key in self.__Data:
            yield key

    def __contains__(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_database_selected()
        return key in self.__Data

    def database(self, name: str):
        if type(name) is not str:
            raise TypeError(f'Wrong type: The database\'s name needs to be a string')
        if name == '':
            raise ValueError(f'Wrong value: The database\'s name can not be an empty string')
        self.__Database = name
        self.__Data = self.__load__(f'{self.__Path}/{self.__Database}.json')
        if name not in self.__Config['Databases']:
            self.__Config['Databases'].append(name)
            self.__save_config()
        return self

    def delete(self, database_name: str = None):
        self.__Database = None

    def pop(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_database_selected()
        try:
            return self.__Data.pop(key)
        except KeyError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Database}" database') from None
        except TypeError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Database}" database') from None

    def commit(self):
        self.__save__(f'{self.__Path}/{self.__Database}.json', self.__Data)

    def __validate_database_selected(self):
        if self.__Database is None:
            raise NameError('Database not found: No database selected, use "database(name: string)" to select one')

    @staticmethod
    def __validate_key_type(key):
        if type(key) not in [str, int, float]:
            raise TypeError(f'Wrong type: The key "{key}" needs to be a string, float or integer')

    @staticmethod
    def __save__(path: str, data, indent: int | None = None):
        finished = False
        file_locked = open(path, 'w')
        while not finished:
            try:
                msvcrt.locking(file_locked.fileno(), msvcrt.LK_NBLCK, os.path.getsize(path))
                json.dump(data, file_locked, indent=indent)
            except IOError:
                pass
            finally:
                msvcrt.locking(file_locked.fileno(), msvcrt.LK_UNLCK, os.path.getsize(path))
                file_locked.close()
                finished = True

    @staticmethod
    def __load__(path: str):
        try:
            with open(path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            open(path, 'w+').close()
            return {}
        except json.decoder.JSONDecodeError:
            path = path.replace('/', '\\')
            file = path.split('\\')[-1]
            print(f'Warning: The file "{file}" could not be decoded, it will be recreated from the beginning')
            return {}

    def __load_config(self):
        config = self.__load__('config.json')
        config = {} if config is None else config
        edited = False
        if 'Databases' not in config:
            edited = True
            config['Databases'] = []

        if edited:
            self.__save_config()
        return config

    def __save_config(self):
        self.__save__('config.json', self.__Config, 4)

    @property
    def databases(self):
        return self.__Config['Databases']

    @property
    def values(self):
        return self.__Data.values()

    @property
    def keys(self):
        return self.__Data.keys()
