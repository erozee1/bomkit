"""BOM schema definitions for standard template headers and column mappings."""

from typing import Dict, List, Any

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
        "mpn", "mfg pn", "mfg_pn", "mfr pn", "mfr_pn", "part number",
        "p/n", "pn", "mfr_part_no"
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

# Canonical field schemas for each BOM header
CANONICAL_FIELDS: List[Dict[str, Any]] = [
    {
        "id": "part_number",
        "label": "Part Number",
        "aliases": [
            "part number", "part_number", "partnumber", "part no", "part_no",
            "part#", "part #", "item number", "item_number", "itemnumber",
            "item no", "item_no", "item#", "item #", "component id",
            "component_id", "componentid", "part id", "part_id", "partid"
        ],
        "expected": {
            "kind": "string",
            "patterns": ["^[A-Za-z0-9][A-Za-z0-9\\-\\._/]{0,127}$"]
        },
        "examples": [
            "C_Small",
            "R_Small",
            "LED",
            "74LS138",
            "Conn_01x40"
        ],
        "description": "Internal part identifier or component designation used within the BOM. May be a library reference, component name, or internal part code."
    },
    {
        "id": "description",
        "label": "Description",
        "aliases": [
            "description", "part description", "part_description",
            "partdescription", "component name", "component_name",
            "componentname", "name", "part name", "part_name",
            "partname", "designation", "comment", "comments"
        ],
        "expected": {
            "kind": "string",
            "patterns": ["^.{0,500}$"]
        },
        "examples": [
            "Unpolarized capacitor, small symbol",
            "Light emitting diode",
            "Resistor, small symbol",
            "Decoder 3 to 8 active low outputs",
            "Generic connector, double row, 02x25, odd/even pin numbering scheme"
        ],
        "description": "Human-readable description of the component, including its function, type, and key characteristics."
    },
    {
        "id": "quantity",
        "label": "Quantity",
        "aliases": [
            "quantity", "qty", "qty.", "qnty", "count", "amount", "qty required",
            "qty_required", "qtyrequired", "required qty", "required_qty"
        ],
        "expected": {
            "kind": "integer",
            "patterns": ["^[1-9]\\d*$"]
        },
        "examples": [
            "1",
            "2",
            "3",
            "8"
        ],
        "description": "The number of units of this component required in the assembly. Must be a positive integer."
    },
    {
        "id": "unit",
        "label": "Unit of Measure",
        "aliases": [
            "unit", "unit of measure", "unit_of_measure", "unitofmeasure",
            "uom", "units", "measure", "measurement"
        ],
        "expected": {
            "kind": "string",
            "patterns": ["^(pcs?|pieces?|ea|each|unit|units)?$"]
        },
        "examples": [
            "pcs",
            "pieces",
            "ea",
            "each",
            "unit"
        ],
        "description": "The unit of measure for the quantity. Typically 'pcs', 'pieces', 'ea', 'each', or 'unit'. May be empty if quantity is implicitly in pieces."
    },
    {
        "id": "manufacturer",
        "label": "Manufacturer",
        "aliases": [
            "manufacturer", "mfg", "mfg.", "vendor", "supplier",
            "manufacturer name", "manufacturer_name", "manufacturername",
            "vendor name", "vendor_name", "vendorname", "supplier name",
            "supplier_name", "suppliername"
        ],
        "expected": {
            "kind": "string",
            "patterns": ["^[A-Za-z0-9][A-Za-z0-9\\s\\-\\._&]{0,127}$"]
        },
        "examples": [
            "Texas Instruments",
            "STMicroelectronics",
            "Vishay",
            "Murata",
            "Panasonic"
        ],
        "description": "The name of the component manufacturer or vendor. This is the company that produces the part, not the distributor."
    },
    {
        "id": "manufacturer_part_number",
        "label": "Manufacturer Part Number",
        "aliases": [
            "manufacturer part number", "manufacturer_part_number",
            "manufacturerpartnumber", "mfg part number", "mfg_part_number",
            "mfgpartnumber", "mfg part no", "mfg_part_no", "mfgpartno",
            "vendor part number", "vendor_part_number", "vendorpartnumber",
            "supplier part number", "supplier_part_number", "supplierpartnumber",
            "mpn", "mfg pn", "mfg_pn", "mfr pn", "mfr_pn", "part number",
            "p/n", "pn", "mfr_part_no"
        ],
        "expected": {
            "kind": "string",
            "patterns": ["^[A-Za-z0-9][A-Za-z0-9\\-\\._/]{1,64}$"]
        },
        "examples": [
            "RC0603FR-0710KL",
            "STM32F103C8T6",
            "LM358",
            "1N4148",
            "2N3904"
        ],
        "description": "The manufacturer's identifier for the part (not distributor SKU). This is the official part number assigned by the manufacturer."
    },
    {
        "id": "reference_designator",
        "label": "Reference Designator",
        "aliases": [
            "reference designator", "reference_designator",
            "referencedesignator", "ref des", "ref_des", "refdes",
            "reference", "references", "designator", "designators",
            "ref", "refs", "designation", "designations"
        ],
        "expected": {
            "kind": "string",
            "patterns": [
                "^[A-Z]\\d+(-[A-Z]\\d+)?(, [A-Z]\\d+(-[A-Z]\\d+)?)*$",
                "^[A-Z]\\d+(, [A-Z]\\d+)*$"
            ]
        },
        "examples": [
            "R1-R3",
            "C1, C2, C4",
            "D1-D8",
            "U1",
            "J1, J2, J3"
        ],
        "description": "Schematic reference designators indicating where components are placed on the PCB. May be a single designator (e.g., 'U1'), a range (e.g., 'R1-R3'), or a comma-separated list (e.g., 'C1, C2, C4')."
    },
    {
        "id": "value",
        "label": "Value",
        "aliases": [
            "value", "specification", "spec", "specs", "rating",
            "electrical value", "electrical_value", "electricalvalue",
            "parameter", "parameters"
        ],
        "expected": {
            "kind": "string",
            "patterns": [
                "^[\\d.]+\\s*[pnumkMG]?[A-Za-zΩ]+$",
                "^[\\d.]+$",
                "^[A-Za-z0-9\\-]+$"
            ]
        },
        "examples": [
            "10n",
            "100n",
            "1k",
            "470R",
            "LED",
            "3.3V",
            "100kΩ"
        ],
        "description": "The electrical value or specification of the component. For passive components (resistors, capacitors, inductors), this is typically the component value with optional unit (e.g., '10nF', '1kΩ', '100uH'). For active components, this may be a part number or key specification."
    },
    {
        "id": "package",
        "label": "Package",
        "aliases": [
            "package", "package type", "package_type", "packagetype",
            "footprint", "footprints", "case", "case type", "case_type",
            "casetype", "housing", "housing type", "housing_type"
        ],
        "expected": {
            "kind": "string",
            "patterns": ["^[A-Za-z0-9][A-Za-z0-9\\-\\._/:]{0,127}$"]
        },
        "examples": [
            "Capacitor_THT:C_Disc_D4.7mm_W2.5mm_P5.00mm",
            "Resistor_THT:R_Axial_DIN0207_L6.3mm_D2.5mm_P7.62mm_Horizontal",
            "Package_DIP:DIP-16_W7.62mm_Socket",
            "0805",
            "SOIC-8",
            "DIP-14"
        ],
        "description": "The physical package type, footprint, or case style of the component. This describes the physical form factor and mounting style (e.g., '0805', 'DIP-14', 'SOIC-8', or full footprint library path)."
    },
    {
        "id": "notes",
        "label": "Notes",
        "aliases": [
            "notes", "note", "comments", "comment", "remarks", "remark",
            "additional info", "additional_info", "additionalinfo",
            "misc", "miscellaneous", "other", "other info", "other_info"
        ],
        "expected": {
            "kind": "string",
            "patterns": ["^.{0,1000}$"]
        },
        "examples": [
            "RoHS compliant",
            "Alternative: LM358",
            "Tolerance: 5%",
            "Temperature range: -40°C to +85°C"
        ],
        "description": "Additional notes, comments, or remarks about the component. May include special requirements, alternatives, tolerances, temperature ratings, or other relevant information."
    }
]

# Create a lookup dictionary by field ID for easy access
FIELD_SCHEMAS: Dict[str, Dict[str, Any]] = {
    field["id"]: field for field in CANONICAL_FIELDS
}

__all__ = [
    "STANDARD_HEADERS",
    "COLUMN_MAPPINGS",
    "CANONICAL_FIELDS",
    "FIELD_SCHEMAS"
]
