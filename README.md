# 🚦 PIPELINE PRF - ANÁLISE DE ACIDENTES 2025

## ✅ PROJETO CONCLUÍDO

Pipeline completo de ETL para análise de acidentes da PRF em 2025.

---

## 📂 ARQUIVO ÚNICO PARA DASHBOARD

**Você precisa apenas deste arquivo:**

```
📁 data/final/
  └─ dashboard_unificado.csv (38 MB)
```

### 📊 Estatísticas
- **Linhas**: 53.213 acidentes
- **Colunas**: 98 campos
- **Período**: Jan-Set 2025
- **Tamanho**: 37.28 MB

---

## 🎯 COMO USAR

### 1. Conectar ao Dashboard

**Power BI:**
```
Home → Get Data → Text/CSV → Selecionar: dashboard_unificado.csv
```

**Tableau:**
```
Connect → Text file → Selecionar: dashboard_unificado.csv
```

### 2. Criar Visualizações

O arquivo contém tudo que você precisa:

✅ **Dados detalhados** de cada acidente  
✅ **Métricas agregadas** para contexto  
✅ **Scores de risco** (0-100)  
✅ **Rankings** automáticos  
✅ **Coordenadas** para mapas (lat/long)  
✅ **Flags** para filtros rápidos  

### 3. Respostas Rápidas

Use os campos `is_worst_*` para criar cards:

- `is_worst_hour = TRUE` → **Pior horário: 18h**
- `is_worst_dow = TRUE` → **Pior dia: Sábado**
- `is_worst_state = TRUE` → **Pior estado: MG**
- `is_worst_highway = TRUE` → **Pior rodovia: BR-101**

---

## 📋 PRINCIPAIS CAMPOS

### Identificação
- `id`, `date`, `hour`, `datetime`

### Localização
- `uf`, `br`, `km`, `municipio`
- `latitude`, `longitude` (para mapas!)

### Severidade
- `severity_score` (0-100)
- `mortos`, `feridos`, `ilesos`
- `has_deaths`, `has_injuries` (boolean)

### Risco
- `composite_risk_score` (0-100) - **Score principal**
- `time_risk_score`, `location_risk_score`, `condition_risk_score`
- `is_high_risk` (boolean)

### Temporal
- `day_of_week_name_pt` (nome do dia em português)
- `time_period` (manhã, tarde, noite, madrugada)
- `is_weekend`, `is_rush_hour` (boolean)

### Condições
- `condicao_metereologica`, `tipo_pista`, `tracado_via`
- `weather_related`, `alcohol_involved`, `speed_related` (boolean)

### Visualização
- `marker_color` (cor para pontos no mapa)
- `marker_size` (tamanho para pontos)
- `is_hotspot` (pontos críticos)

### Métricas Agregadas
- `accidents_this_hour` (total de acidentes nesta hora)
- `accidents_this_dow` (total neste dia da semana)
- `accidents_in_state` (total neste estado)
- `accidents_on_highway` (total nesta rodovia)

---

## 📚 DOCUMENTAÇÃO COMPLETA

Leia os arquivos na pasta `data/final/`:

- **`LEIA_ME.txt`** - Guia rápido de uso
- **`data_dictionary.txt`** - Dicionário completo de campos
- **`metadata.json`** - Metadados técnicos

---

## 🗂️ ESTRUTURA DO PROJETO

```
dados_abertos_prf/
├── data/
│   ├── raw/                    # Dados originais (não modificar)
│   ├── final/                  # ⭐ ARQUIVO ÚNICO AQUI
│   │   ├── dashboard_unificado.csv  ← USAR ESTE!
│   │   ├── LEIA_ME.txt
│   │   ├── data_dictionary.txt
│   │   └── metadata.json
│   └── backup_arquivos_intermediarios/  # Backup dos arquivos originais
│
├── extract/                    # Módulo de extração
├── transform/                  # Módulos de transformação
├── load/                       # Módulo de exportação
├── utils/                      # Utilitários
│
├── pipeline.py                 # Script principal do pipeline
├── requirements.txt            # Dependências Python
└── README.md                   # Este arquivo
```

---

## 🔧 INFORMAÇÕES TÉCNICAS

### Dependências
```bash
pip install pandas numpy scikit-learn python-dateutil
```

### Re-executar Pipeline (se necessário)
```bash
python3 pipeline.py
```

### Pipeline Modules
1. **Extract** - Carrega dados raw (CSV)
2. **Clean** - Limpa e padroniza dados
3. **Enrich** - Adiciona campos calculados
4. **Calculate Risks** - Calcula scores de risco
5. **Geographic Analysis** - Análise geoespacial e clusters
6. **Aggregate** - Cria agregações
7. **Export** - Exporta arquivos finais

---

## 🎨 EXEMPLOS DE ANÁLISES

### 1. Mapa de Acidentes
```
Campo X: longitude
Campo Y: latitude
Cor: marker_color
Tamanho: marker_size
Filtro: has_deaths = TRUE
```

### 2. Gráfico Temporal
```
Eixo X: hour
Eixo Y: COUNT(id)
Série: day_of_week_name_pt
```

### 3. Top 10 Estados
```
Dimensão: uf
Métrica: accidents_in_state
Ordenar: DESC
Limite: 10
```

### 4. Análise de Risco
```
Eixo X: composite_risk_score (bins: 0-25, 26-50, 51-75, 76-100)
Eixo Y: COUNT(id)
Cor por: has_deaths
```

### 5. Hotspots (Pontos Críticos)
```
Filtro: is_hotspot = TRUE
Mapa: latitude, longitude
Cor: severity_score
```

---

## 📊 ESTATÍSTICAS DO DATASET

- **Total de Acidentes**: 53.213
- **Total de Mortes**: 4.473
- **Total de Feridos**: 61.040
- **Estados**: 27
- **Rodovias**: 112
- **Cidades**: 1.790
- **Período**: 01/01/2025 - 30/09/2025

---

## 🎯 RESPOSTAS PRINCIPAIS

| Pergunta | Resposta |
|----------|----------|
| **Pior horário para dirigir?** | 18h (6 da tarde) |
| **Pior dia da semana?** | Sábado |
| **Estado mais perigoso?** | MG (Minas Gerais) |
| **Rodovia mais perigosa?** | BR-101 |

---

## ✨ RECURSOS ESPECIAIS

### Filtros Prontos
- `has_deaths` - Acidentes com mortes
- `has_injuries` - Acidentes com feridos
- `is_high_risk` - Acidentes de alto risco
- `is_weekend` - Fins de semana
- `is_rush_hour` - Horários de pico
- `is_hotspot` - Pontos críticos
- `alcohol_involved` - Envolvimento de álcool
- `weather_related` - Relacionado ao clima

### Rankings Automáticos
- `hour_danger_rank` - Ranking de perigo por hora
- `day_danger_rank` - Ranking por dia
- `state_danger_rank` - Ranking por estado
- `highway_danger_rank` - Ranking por rodovia
- `danger_percentile` - Percentil geral de perigo (0-100)

### Contexto Agregado
Cada linha tem informações agregadas para comparação:
- Quantos acidentes aconteceram nesta hora?
- Quantos neste dia da semana?
- Quantos neste estado?
- Quantos nesta rodovia?

---

## 🚀 PRÓXIMOS PASSOS

1. ✅ Abra seu BI preferido (Power BI / Tableau)
2. ✅ Conecte o arquivo `dashboard_unificado.csv`
3. ✅ Crie visualizações usando os campos sugeridos
4. ✅ Use os filtros prontos (`is_*`, `has_*`)
5. ✅ Publique e compartilhe!

---

## 📞 INFORMAÇÕES

**Projeto**: Pipeline ETL PRF  
**Versão**: 1.0  
**Data**: 15/11/2025  
**Período dos Dados**: Jan-Set 2025  
**Fonte**: Dados Abertos PRF

---

## 🎉 TUDO PRONTO!

**Você tem um arquivo único e completo.**  
**Não precisa de mais nada.**  
**Conecte e crie visualizações incríveis! 🚀**

