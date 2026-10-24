# Simulador de Processamento Distribuído com PySpark

Projeto desenvolvido para a disciplina Processamento de Grandes Volumes de Dados, implementando simulações práticas do ecossistema **Apache Spark** (inspiração baseada no repositório `akshat-rastogi/spark-basics`). O projeto aborda arquitetura distribuída, avaliação preguiçosa (*Lazy Evaluation*), linhagem de dados (*DAG*), além dos módulos de Spark SQL, MLlib, GraphX e Structured Streaming.

---

## Integrantes do Grupo
* **Derek da Penha Teixeira**
* **Matheus Santana Costa Leite**
* **Roberto de Andrade Pires Neto**
* **Victor Machado Martins**

---

## Estrutura do Projeto

O projeto possui scripts executáveis na pasta `pyspark/` e relatórios interativos integrados com cenários reais de mercado na pasta `Cenarios_Reais/` (Jupyter Notebooks):
1. **`spark_sql.py` / `spark_sql.ipynb`** — Processamento relacional de DataFrames e consultas ANSI SQL in-memory.
2. **`spark_mllib.py` / `spark_mllib.ipynb`** — Machine learning distribuído com persistência de iterações em cache na RAM.
3. **`spark_graph_analysis.py` / `spark_graph_analysis.ipynb`** — Análise de redes e grafos relacionais (Conceito GraphX).
4. **`spark_structured_streaming.py` / `spark_structured_streaming.ipynb`** — Ingestão de fluxo contínuo de dados em tempo real (*Tabela Infinita*).

---

## Pré-requisitos e Instalação

O projeto foi construído utilizando as seguintes versões de suporte:
* **Python** 3.14.6
* **Java** 17 (HotSpot)
* **Scala** 2.13.18
* **Apache Spark** 4.2.0 + Hadoop 3.5.0

### Configuração com Invoke

Para configurar o ambiente de desenvolvimento e instalar o pacote em modo editável junto com as dependências de desenvolvimento e teste, utilize o comando gerenciado pelo `tasks.py`:

```bash
invoke install
```

# Executando as Simulações

Cada módulo pode ser executado de forma independente para demonstrações práticas ao vivo:

### Pilar 1: Spark SQL
```bash
python pyspark/spark_sql.py
```
### Pilar 2: MLlib (Regressão Linear)
```bash
python pyspark/spark_mllib.py
```
### Pilar 3: Análise de Grafos / Redes
```bash
python pyspark/spark_graph_analysis.py
```
### Pilar 4: Structured Streaming (Tempo Real)
```bash
python pyspark/spark_structured_streaming.py
```

### Conceitos Abordados
* **Arquitetura Distribuída**: Papéis do Driver, Cluster Manager e Executors orquestrando Threads concorrentes na JVM.

* **Avaliação Preguiçosa & DAG**: Otimização lógica através do Catalyst Optimizer e execução tardia disparada apenas por Ações (Actions).

* **Tolerância a Falhas**: Recomputação baseada em linhagem de RDDs aplicada em cenários de nuvem (Google Cloud Dataproc com instâncias Spot/Preemptibles).