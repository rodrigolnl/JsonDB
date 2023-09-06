import json
import os
import msvcrt
import time
import warnings


class JsonDB:
    def __init__(self, repository: str, path: str = None):
        base_path = str(os.path.dirname(os.path.abspath(__file__))) + '\\repo'
        if not path:
            try:
                os.mkdir(base_path)
            except FileExistsError:
                pass
        self.__Path = f'{path if path else base_path}\\{repository}'
        self.__Table = None
        self.__Data: dict[str | int | float] = {}
        self.__Config = {}
        self.__load_config()
        self.__identity = None
        if repository not in self.__Config['Repositories']:
            self.__Config['Repositories'][repository] = self.__Path
            self.__save_config()
        else:
            self.__Path = self.__Config['Repositories'][repository]

        try:
            os.mkdir(self.__Path)
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
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Table}" database') from None

    def __setitem__(self, key: str | int | float, value):
        if key == '_IDENTITY':
            try:
                self.__identity = int(self.__identity)
            except ValueError:
                self.__identity = 1
            while str(self.__identity) in self.__Data:
                self.__identity += 1
            key = self.__identity

        self.__validate_key_type(key)
        self.__validate_database_selected()
        key = str(key)
        try:
            self.__Data[key] = value
        except KeyError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Table}" database') from None
        except TypeError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Table}" database') from None

    def __delitem__(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_database_selected()
        try:
            del self.__Data[key]
        except KeyError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Table}" database') from None

    def __iter__(self):
        self.__validate_database_selected()
        for key in self.__Data:
            yield self.__Data[key]

    def __contains__(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_database_selected()
        return key in self.__Data

    def collection(self, name: str):
        if type(name) is not str:
            raise TypeError(f'Wrong type: The table\'s name needs to be a string')
        if name == '':
            raise ValueError(f'Wrong value: The table\'s name can not be an empty string')
        self.__Table = name
        self.__Data = self.__load__(f'{self.__Path}/{self.__Table}.json')
        if name not in self.__Config['Collections']:
            self.__Config['Collections'].append(name)
            self.__save_config()

        keys = list(self.__Data.keys())
        self.__identity = keys[-1] if keys else 1
        return self

    def delete(self, database_name: str = None):
        self.__Table = None

    def pop(self, key: str | int | float):
        self.__validate_key_type(key)
        self.__validate_database_selected()
        try:
            return self.__Data.pop(key)
        except KeyError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Table}" database') from None
        except TypeError:
            raise KeyError(f'Key not found: The key "{key}" is not a valid key in "{self.__Table}" database') from None

    def commit(self):
        self.__save__(f'{self.__Path}/{self.__Table}.json', self.__Data)

    def __validate_database_selected(self):
        if self.__Table is None:
            raise NameError('Table not found: No database selected, use "database(name: string)" to select one')

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
                teste = os.path.getsize(path)
                print(teste)
                msvcrt.locking(file_locked.fileno(), msvcrt.LK_NBLCK, os.path.getsize(path))
                json.dump(data, file_locked, indent=indent)
            except Exception as e:
                print(e)
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
            warnings.formatwarning = JsonDB.__custom_formatwarning
            warnings.warn(f'The file "{file}" could not be decoded or is empty, it will be recreated!')
            return {}

    def __load_config(self):
        config = self.__load__('config.json')
        config = {} if config is None else config
        edited = False
        if 'Collections' not in config:
            edited = True
            config['Collections'] = []
        if 'Repositories' not in config:
            edited = True
            config['Repositories'] = {}

        if edited:
            self.__save_config()
        self.__Config = config

    def __save_config(self):
        self.__save__('config.json', self.__Config, 4)

    @staticmethod
    def __custom_formatwarning(msg, *args, **kwargs):
        return str(msg) + '\n'

    @property
    def all_collections(self):
        return self.__Config['Collections']

    @property
    def values(self):
        return self.__Data.values()

    @property
    def keys(self):
        return self.__Data.keys()
