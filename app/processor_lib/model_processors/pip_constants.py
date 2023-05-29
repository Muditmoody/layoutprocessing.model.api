class Constants:
    CauseCodeMap = {
        "CauseCodeId": "CauseCodeId",
        "CauseText": "CauseText",
        "CauseCode": "CauseCode"
    }

    GroupCodeMap = {
        "CodeGroupId": "GroupCodeId",
        "CodeGroupName": "GroupCode",
        "CodeGroupText": "GroupText"
    }

    CodingCodeMap = {
        "CodingCodeId": "CodingCodeId",
        "CodingCodeName": "Coding",
        "CodingCodeText": "CodingText"
    }

    DamageCodeMap = {
        "DamageCodeId": "DamageCodeId",
        "DamageCodeName": "DamageCode",
        "DamageCodeText": "DamageText"
    }

    EngineProgramMap = {
        "EngineProgramId": "EngineProgramId",
        "NotificationCode": "Notification",
        "Description": "Description",
        "CodingCodeId": "Coding"
    }

    MaterialMap = {
        "MaterialId": "MaterialId",
        "MaterialCode": "Material",
        "Description": "Description"
    }

    SupplierVendorMap = {
        "SupplierVendorId": "SupplierVendorId",
        "SupplierVendorCode": "SupplierVendor"
    }

    # GroupCodeName might be redundant
    TaskCodeMap = {
        "TaskCodeId": "TaskCodeId",
        "TaskCodeName": "TaskCode",
        "TaskCodeText": "TaskCodeText",
        "GroupCodeId": "GroupCodeId",
        "GroupCodeName": "GroupCode"
    }

    TaskOwnerMap = {
        "TaskOwnerId": "TaskOwnerId",
        "TaskOwnerCode": "TaskOwner"
    }

    TaskStatusMap = {
        "TaskStatusId": "TaskStatusId",
        "TaskStatusCode": "TaskStatus"
    }
    # LayoutType:
    LayoutTypeMap = {
        "LayoutTypeId": "LayoutTypeId",
        "NotificationId": "Notification",
        "ItemNumber": "Sort_Number",
        "LayoutText": "TaskText",
        "DamageCodeId": "DamageCodeId",
        "DamageCodeName": "DamageCode",
        "CauseCodeId": "CauseCodeId",
        "CauseCodeName": "CauseCode"
    }

    # Layout: originally it's not id's, but here we do not need to add more aggregation
    LayoutProcessingTaskMap = {
        "LayoutTaskId": "LayoutTaskId",
        "LayoutId": "LayoutId",
        "TaskId": "Task",
        "ItemNumber": "Item",
        "NotificationId": "NotificationId",
        "NotificationCode": "Notification",
        "MaterialId": "MaterialId",
        "MaterialCode": "Material",
        "CreatedOn": "Created_On",
        "CompletedOn": "Completed_On",
        "TaskText": "TaskText",
        "TaskCodeId": "TaskCodeId",
        # "TaskCodeName": "TaskCode",
        "PlannedStart": "Planned_Start",
        "PlannedFinish": "Planned_Finish",
        "SupplierVendorId": "SupplierVendorId",
        # "SupplierVendorCode": "SupplierVendor",
        "TaskOwnerId": "TaskOwnerId",
        # "TaskOwnerCode": "TaskOwner",
        "TaskStatusId": "TaskStatusId",
        # "TaskStatusCode": "TaskStatus",
        "CategoryId": "CategoryId",  # might need modification
        "DamageCodeId": "DamageCodeId",
        "CauseCodeId": "CauseCodeId",
        "CodeGroupId": "GroupCodeId"
    }

    # not added to api yet
    CategoryMap = {
        "CategoryId": "CategoryId",
        "CategoryName": "Category"
    }

    GeneralCodeMap = {
        "GeneralCodeId": "GeneralCodeId",
        "CodeGroup": "Code_Group",
        "TaskCode": "Task_Code",
        "GeneralCode": "General_Code"
    }

    CleaningSourceMap = {
        "GroupCode": {
            "request_url": 'http://localhost:6001/api/etl/CodeGroup/GetGroupCodes',
            "columnMap": GroupCodeMap,
            "drop_columns": [],
            "is_active": True
        },

        "CodingCode": {
            "request_url": 'http://localhost:6001/api/etl/CodingCode/GetCodingCodes',
            "columnMap": CodingCodeMap,
            "drop_columns": [],
            "is_active": True
        },

        "SupplierVendor": {
            "request_url": 'http://localhost:6001/api/etl/SupplierVendor/GetSupplierVendors',
            "columnMap": SupplierVendorMap,
            "drop_columns": [],
            "is_active": True
        },

        "DamageCode": {
            "request_url": 'http://localhost:6001/api/etl/DamageCode/GetDamageCodes',
            "columnMap": DamageCodeMap,
            "drop_columns": [],
            "is_active": True
        },

        "CauseCode": {
            "request_url": 'http://localhost:6001/api/etl/CauseCode/GetCauseCodes',
            "columnMap": CauseCodeMap,
            "drop_columns": [],
            "is_active": True
        },

        "TaskOwner": {
            "request_url": 'http://localhost:6001/api/etl/TaskOwner/GetTaskOwners',
            "columnMap": TaskOwnerMap,
            "drop_columns": [],
            "is_active": True
        },

        "TaskStatus": {
            "request_url": 'http://localhost:6001/api/etl/TaskStatus/GetTaskStatuses',
            "columnMap": TaskStatusMap,
            "drop_columns": [],
            "is_active": True
        },

        "EngineProgram": {
            "request_url": 'http://localhost:6001/api/etl/EngineProgram/GetEnginePrograms',
            "columnMap": EngineProgramMap,
            "drop_columns": [],
            "is_active": True
        },

        "Material": {
            "request_url": 'http://localhost:6001/api/etl/Material/GetMaterials',
            "columnMap": MaterialMap,
            "drop_columns": [],
            "is_active": True
        },

        "LayoutType": {
            "request_url": 'http://localhost:6001/api/etl/LayoutType/GetLayoutTypes',
            "columnMap": LayoutTypeMap,
            "drop_columns": [],
            "is_active": True
        },

        "TaskCode": {
            "request_url": 'http://localhost:6001/api/etl/TaskCode/GetTaskCodes',
            "columnMap": TaskCodeMap,
            "drop_columns": [],
            "is_active": True
        },

        "LayoutProcessingTask": {
            "request_url": 'http://localhost:6001/api/etl/LayoutTask/GetLayoutTasks',
            "columnMap": LayoutProcessingTaskMap,
            "drop_columns": [],
            "is_active": True
        },

        "GeneralCode": {
            "request_url": 'https://localhost:5001/api/model/ModelRefData/GetGroupTaskCodeMatchMap',
            "columnMap": GeneralCodeMap,
            "drop_columns": [],
            "is_active": True
        },

        "Category": {
            "request_url": 'https://localhost:5001/api/etl/Category/GetCategory',
            "columnMap": CategoryMap,
            "drop_columns": [],
            "is_active": True
        },
    }

    train_data_url = 'https://localhost:5001/api/model/ModelDataProcessed/GetModelCleanData?train=true'

    inference_data_url = 'https://localhost:5001/api/model/ModelDataProcessed/GetModelCleanData?train=false'

    cleanModelInputColMap = {
        "Notification": "notificationCode",
        "Item"  : "itemNumber",
        "LayoutTaskId": "layoutTaskId",
        "Task" : "taskNumber",
        "Material" : "materialCode",
        "Created_On" :  "createdDate",
        "Completed_On" :  "completedDate",
        "TaskText": "taskText",
        "TaskCodeId" : "taskCodeId",
        "Planned_Start" : "plannedStart",
        "Planned_Finish" :"plannedFinish",
        "SupplierVendorId" :  "supplierVendorId",
        "TaskOwnerId" :  "taskOwnerId",
        "TaskStatusId" : "taskStatusId",
        "DamageCodeId" : "damageCodeId",
        "CauseCodeId" : "causeCodeId",
        "GroupCodeId" : "groupCodeId",
        "CategoryId" : "categoryId",
        "Coding" : "codingCodeId",
        "Engine" : "engineProgram",
        "SupplierVendor"  : "supplierVendorCode",
        "TaskOwner" : "taskOwnerCode",
        "TaskStatus" : "taskStatusCode",
        "GroupCode" : "groupCode" ,
        "CauseCode" : "causeCode",
        "DamageCode" :  "damageCode",
        "Category" :  "category",
        "TaskCode" : "taskCode",
        "TaskCodeText" : "taskCodeText",
        "GeneralCode" : "generalCode",
        "IsTaskCompleted" : "isTaskCompleted",
        "IsItemCompleted" : "isItemCompleted",
        "IsTurnback" : "isTurnback",
        "IsLife" : "isLife",
        "IsPlanning" : "isPlanning",
        "Available" :   "available"

    }