"""BOM schema definitions for standard template headers and column mappings."""

# Standard BOM template headers in order
STANDARD_HEADERS = [
    "part_number",
    "description",
    "quantity",
    "unit",
    "manufacturer",
    "manufacturer_part_number",
    "reference_designator",
    "value",
    "package",
    "notes"
]

# Mapping of common column name variations to standard headers
COLUMN_MAPPINGS = {
    "part_number": [
        "part number", "part_number", "partnumber", "part no", "part_no",
        "part#", "part #", "item number", "item_number", "itemnumber",
        "item no", "item_no", "item#", "item #", "component id",
        "component_id", "componentid", "part id", "part_id", "partid"
    ],
    "description": [
        "description", "part description", "part_description",
        "partdescription", "component name", "component_name",
        "componentname", "name", "part name", "part_name",
        "partname", "designation", "comment", "comments"
    ],
    "quantity": [
        "quantity", "qty", "qty.", "qnty", "count", "amount", "qty required",
        "qty_required", "qtyrequired", "required qty", "required_qty"
    ],
    "unit": [
        "unit", "unit of measure", "unit_of_measure", "unitofmeasure",
        "uom", "units", "measure", "measurement"
    ],
    "manufacturer": [
        "manufacturer", "mfg", "mfg.", "vendor", "supplier",
        "manufacturer name", "manufacturer_name", "manufacturername",
        "vendor name", "vendor_name", "vendorname", "supplier name",
        "supplier_name", "suppliername"
    ],
    "manufacturer_part_number": [
        "manufacturer part number", "manufacturer_part_number",
        "manufacturerpartnumber", "mfg part number", "mfg_part_number",
        "mfgpartnumber", "mfg part no", "mfg_part_no", "mfgpartno",
        "vendor part number", "vendor_part_number", "vendorpartnumber",
        "supplier part number", "supplier_part_number", "supplierpartnumber",
        "mpn", "mfg pn", "mfg_pn"
    ],
    "reference_designator": [
        "reference designator", "reference_designator",
        "referencedesignator", "ref des", "ref_des", "refdes",
        "reference", "references", "designator", "designators",
        "ref", "refs", "designation", "designations"
    ],
    "value": [
        "value", "specification", "spec", "specs", "rating",
        "electrical value", "electrical_value", "electricalvalue",
        "parameter", "parameters"
    ],
    "package": [
        "package", "package type", "package_type", "packagetype",
        "footprint", "footprints", "case", "case type", "case_type",
        "casetype", "housing", "housing type", "housing_type"
    ],
    "notes": [
        "notes", "note", "comments", "comment", "remarks", "remark",
        "additional info", "additional_info", "additionalinfo",
        "misc", "miscellaneous", "other", "other info", "other_info"
    ]
}

__all__ = ["STANDARD_HEADERS", "COLUMN_MAPPINGS"]

