from pydantic import BaseModel


class DispatchScadaModel(BaseModel):
    dispatch: str
    unit_scada: str
    settlementdate: str
    duid: str
    scadavalue: str
    lastchanged: str
