from DataLocalizeModule.UpdaterOrigin import UpdaterOrigin
from DataLocalizeModule.UpdaterQuick import UpdaterQuick
from DataProcessModule.BaseDataProcessor import BaseDataProcessor


if __name__=='__main__':
    # update origin
    obj = UpdaterOrigin()
    obj.update_basic_info()
    obj.update_trade_info()
    obj.update_eod_derived()    # h5
    obj.update_st_info()
    # update quick
    obj = UpdaterQuick()
    obj.update_trade_dates_h5()
    obj.update_trade_info_h5()
    obj.update_quick_tables(updateH5=True)
    # basedata process
    obj = BaseDataProcessor()
    obj.update_stock_count(updateH5=True)
    obj.update_features_filter(updateH5=True)
    obj.update_response_filter(updateH5=True)
    obj.update_response(updateH5=True)

    print(obj.dataConnector.get_last_update('ASHAREEODPRICES', isH5=True, lastID=True))
    print(obj.dataConnector.get_last_update('RESPONSE', isH5=True, lastID=True))
    print(obj.dataConnector.get_last_update('FEATURES_FILTER', isH5=True, lastID=True))
    print(obj.dataConnector.get_last_update('RESPONSE_FILTER', isH5=True, lastID=True))
    print(obj.dataConnector.get_last_update('ASHAREST', isH5=True, lastID=True))