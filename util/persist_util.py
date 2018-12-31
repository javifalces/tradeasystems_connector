import pandas as pd


def save_to_file(object, filePath):
    import joblib
    import os
    upperDirectoryToSave = os.path.dirname(filePath)
    if not os.path.exists(upperDirectoryToSave) and upperDirectoryToSave.strip() != '':
        raise FileNotFoundError('%s not found in path to persist object' % filePath)
    if object is None:
        raise Exception('input object to save_to_file is None')
    joblib.dump(object, filePath)
    return True


def read_text_file_list(path, separator=','):
    import os
    if not os.path.exists(path):
        raise FileNotFoundError('%s not found in path to read txt' % path)

    f = open(path, "r")
    line = f.readline()
    output = line.split(separator)

    return output


def write_list_to_file(listObject, filePath, separator=','):
    import os
    upperDirectoryToSave = os.path.dirname(filePath)
    if not os.path.exists(upperDirectoryToSave) and upperDirectoryToSave.strip() != '':
        raise FileNotFoundError('%s not found in path to persist file' % filePath)
    if listObject is None or len(listObject) == 0:
        raise Exception('input list to write_list_to_file is None or empty')
    stringToWrite = ''
    for element in listObject:
        stringToWrite += element + separator
    stringToWrite = stringToWrite[:-1]  # remove last separator
    text_file = open(filePath, "w")
    text_file.write(stringToWrite)


def load_from_file(path):
    import joblib
    import os
    if not os.path.exists(path):
        raise FileNotFoundError('%s not found in path to load object' % path)
    return joblib.load(path)


def excel_to_dataDict(filePath):
    reader = pd.ExcelFile(filePath)
    sheets = reader.sheet_names
    outputDict = {}
    for sheet in sheets:
        outputDict[sheet] = reader.parse(sheet)
    return outputDict


def dataDict_to_excel(outputDictFinal, filePath):
    import pandas as pd

    def __getSheetName__(key):
        key_list = key.split('_')
        if len(key_list) > 1:
            # Ratios splitted, remove prefix of ratio
            selection = key_list[1:]
            output = '_'.join(selection)
        else:
            output = key
        return output[-30:]

    if outputDictFinal is not None and len(outputDictFinal) > 0:
        # Create a Pandas Excel writer using XlsxWriter as the engine.
        writer = pd.ExcelWriter(filePath, engine='xlsxwriter')
        for key in outputDictFinal.keys():
            df = outputDictFinal[key]
            if isinstance(df, pd.DataFrame):
                # df.to_csv(self.temp_dir+os.sep+key+'.csv')
                key = __getSheetName__(key)
                df.to_excel(writer, sheet_name=key[-30:])
        writer.close()
