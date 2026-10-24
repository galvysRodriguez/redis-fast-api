"""Genera examples/informe_q3.pdf: reporte financiero de 2 páginas con texto y 2 tablas.

Uso: python scripts/make_sample_pdf.py [salida.pdf]
Requiere reportlab (incluido en requirements/dev.txt).
"""

import sys

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

st = getSampleStyleSheet()
grid = TableStyle(
    [
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
    ]
)
story = [
    Paragraph("Informe Financiero Trimestral Q3 2026", st["Title"]),
    Paragraph("1. Resumen ejecutivo", st["Heading2"]),
    Paragraph(
        (
            "Durante el tercer trimestre los ingresos operativos crecieron un 18% interanual, "
            "impulsados por la expansión en mercados de Latinoamérica y la reducción de costos "
            "logísticos. "
        )
        * 3,
        st["BodyText"],
    ),
    Spacer(1, 12),
    Paragraph("Tabla 1. Ingresos por región (millones USD)", st["BodyText"]),
    Table(
        [
            ["Región", "Q1", "Q2", "Q3"],
            ["Norteamérica", "120.5", "131.2", "140.8"],
            ["Latinoamérica", "45.3", "52.1", "61.7"],
            ["Europa", "88.0", "85.4", "90.2"],
        ],
        style=grid,
    ),
    PageBreak(),
    Paragraph("2. Costos y márgenes", st["Heading2"]),
    Paragraph(
        (
            "El margen bruto se ubicó en 42%, mientras que los gastos de personal se mantuvieron "
            "estables. La empresa prevé inversiones en automatización durante el próximo año "
            "fiscal. "
        )
        * 3,
        st["BodyText"],
    ),
    Spacer(1, 12),
    Paragraph("Tabla 2. Estructura de costos", st["BodyText"]),
    Table(
        [
            ["Concepto", "Monto", "% ingresos"],
            ["Personal", "95.0", "32%"],
            ["Logística", "40.2", "14%"],
            ["Marketing", "22.7", "8%"],
            ["Tecnología", "18.9", "6%"],
        ],
        style=grid,
    ),
]
OUT = sys.argv[1] if len(sys.argv) > 1 else "examples/informe_q3.pdf"
SimpleDocTemplate(OUT, pagesize=A4).build(story)
