# 🚀 Pipeline ERP → SQL Server (Python)

Automação para ingestão de dados de vendas e produtos a partir de arquivos Excel exportados de um ERP, com carga incremental em banco SQL Server e consumo posterior no Power BI.

---

## 📌 Objetivo

Automatizar o processo de:

* 📥 Leitura de arquivos Excel do ERP
* 🔄 Tratamento e padronização dos dados
* 🗄️ Carga incremental no SQL Server (sem duplicidade)
* 📊 Atualização automática de dashboards no Power BI

---

## 🛠️ Tecnologias Utilizadas

* Python
* Pandas
* SQLAlchemy
* PyODBC
* Excel (OpenPyXL)
* SQL Server
* Power BI

---

## 📂 Estrutura do Projeto

```
📁 PASTA_ARQUIVOS/
   ├── VendasLoja1.xlsx
   ├── VendasLoja2.xlsx
   ├── ProdutosLoja1.xlsx
   ├── ProdutosLoja2.xlsx

📄 automacaoarquivos.py
📄 carga_erp.log
📄 README.md
```

---

## ⚙️ Configuração

### 1. Ajustar caminho dos arquivos

No código, configure:

```python
PASTA_ARQUIVOS = r"C:\caminho\para\seus\arquivos"
```

---

### 2. Configurar conexão com SQL Server

```python
CONN_STRING = (
    "mssql+pyodbc:///?odbc_connect="
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=SEU_SERVIDOR;"
    "DATABASE=SEU_BANCO;"
    "Trusted_Connection=yes;"
)
```

---

### 3. Instalar dependências

```bash
pip install pandas openpyxl pyodbc sqlalchemy
```

---

## ▶️ Como Executar

1. Coloque os arquivos Excel exportados do ERP na pasta definida
2. Execute o script:

```bash
python automacaoarquivos.py
```

3. Atualize seu relatório no Power BI

---

## 📊 Funcionalidades

### 🔹 Leitura Inteligente de Arquivos

* Identifica automaticamente cabeçalhos dinâmicos
* Suporta diferentes layouts do ERP
* Trata datas no formato Excel

---

### 🔹 Carga Incremental

Evita duplicidade com base em:

* Vendas: `id_loja + data`
* Produtos: `id_loja + data + plu`

---

### 🔹 Tratamento de Dados

* Padronização de colunas
* Conversão de datas
* Limpeza de registros inválidos

---

### 🔹 Logs de Execução

Geração automática de log:

```
carga_erp.log
```

Contém:

* Execução do processo
* Quantidade de registros inseridos
* Erros por arquivo

---

## ⚠️ Boas Práticas

* ❌ Não versionar arquivos `.xlsx`
* ❌ Não versionar arquivos `.log`
* ✅ Usar `.gitignore`

Exemplo:

```
*.log
*.xlsx
__pycache__/
```

---

## 📈 Possíveis Melhorias

* Agendamento automático (Task Scheduler / Airflow)
* Validação de dados antes da carga
* Monitoramento de falhas
* Criação de API para ingestão
* Containerização com Docker

---

## 👨‍💻 Autor

João Witallo

---

## 📌 Observação

Este projeto simula um cenário real de engenharia de dados, sendo ideal para portfólio profissional, demonstrando habilidades em:

* ETL (Extract, Transform, Load)
* Integração com banco de dados
* Automação de processos
* Business Intelligence

---
