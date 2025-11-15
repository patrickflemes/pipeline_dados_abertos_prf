# 📊 GUIA DE MÉTRICAS PARA BI - DASHBOARD PRF

## 📂 ARQUIVO DE DADOS
**Conecte este arquivo único:**
```
data/final/dashboard_unificado.csv
```
- 53.213 linhas (acidentes)
- 98 colunas
- Período: Jan-Set 2025

---

## 🎯 MÉTRICAS PRINCIPAIS

### 1. KPIs (Cards no Topo)

#### Total de Acidentes
```
Métrica: COUNT(id)
Valor: 53.213
```

#### Total de Mortes
```
Métrica: SUM(mortos)
Valor: 4.473
Filtro visual: Cor vermelha
```

#### Total de Feridos
```
Métrica: SUM(feridos)
Valor: 61.040
Filtro visual: Cor laranja
```

#### Taxa de Fatalidade
```
Fórmula: (SUM(mortos) / SUM(pessoas)) * 100
Formato: Percentual com 2 casas
Valor: ~3.7%
```

---

## 📌 RESPOSTAS DIRETAS (Cards Destacados)

### Pior Horário
```
Filtro: is_worst_hour = TRUE
Campo: hour + "h"
Resultado: "18h"
Label: "Pior horário para dirigir"
```

### Pior Dia da Semana
```
Filtro: is_worst_dow = TRUE
Campo: day_of_week_name_pt
Resultado: "Sábado"
Label: "Dia mais perigoso"
```

### Pior Estado
```
Filtro: is_worst_state = TRUE
Campo: uf
Resultado: "MG"
Label: "Estado com mais acidentes"
```

### Pior Rodovia
```
Filtro: is_worst_highway = TRUE
Campo: "BR-" + br
Resultado: "BR-101"
Label: "Rodovia mais perigosa"
```

---

## 📊 VISUALIZAÇÕES E MÉTRICAS

### 1. Gráfico: Acidentes por Hora do Dia
```
Tipo: Gráfico de Linha
Eixo X: hour (0-23)
Eixo Y: COUNT(id)
Série 2: SUM(mortos) (linha secundária)
Pico: 18h com ~4.102 acidentes
```

### 2. Gráfico: Acidentes por Dia da Semana
```
Tipo: Gráfico de Barras Vertical
Eixo X: day_of_week_name_pt
Eixo Y: COUNT(id)
Cor por: has_deaths (vermelho/azul)
Ordem: Segunda → Domingo
```

### 3. Gráfico: Distribuição de Severidade
```
Tipo: Gráfico de Pizza ou Rosca
Segmentos: severity_code
Valores: COUNT(id)
Cores: 
  - Sem vítimas: Verde
  - Com feridos: Laranja
  - Com mortes: Vermelho
```

### 4. Gráfico: Top 10 Estados
```
Tipo: Gráfico de Barras Horizontal
Eixo X: COUNT(id)
Eixo Y: uf
Ordenar: Decrescente
Limite: 10
Usar campo auxiliar: accidents_in_state
```

### 5. Gráfico: Top 10 Rodovias
```
Tipo: Gráfico de Barras Horizontal
Eixo X: COUNT(id)
Eixo Y: "BR-" + br
Ordenar: Decrescente
Limite: 10
Usar campo auxiliar: accidents_on_highway
```

### 6. Gráfico: Evolução Temporal
```
Tipo: Gráfico de Linha com Área
Eixo X: date (agrupado por mês)
Eixo Y: COUNT(id)
Série 2: AVG(composite_risk_score) (linha)
Tendência: Exibir linha de tendência
```

### 7. Heatmap: Dia x Hora
```
Tipo: Matriz/Heatmap
Linhas: day_of_week_name_pt
Colunas: hour
Valores: COUNT(id)
Cor: Gradiente (Verde → Vermelho)
```

### 8. Mapa Geográfico
```
Tipo: Mapa de Pontos
Longitude: longitude
Latitude: latitude
Cor: marker_color (usar como está)
Tamanho: marker_size
Tooltip: 
  - Rodovia: "BR-" + br + " km " + km
  - Local: municipio + ", " + uf
  - Mortos: mortos
  - Feridos: feridos
Filtro: Limite a 10.000 pontos (performance)
```

### 9. Tabela: Trechos Mais Perigosos
```
Tipo: Tabela Interativa
Usar arquivo: highway_segments_risk.csv (do backup)
Colunas:
  - Trecho (highway + " km " + km_start + "-" + km_end)
  - Estado (state)
  - Acidentes (accident_count)
  - Mortes (deaths)
  - Score de Risco (risk_score)
Ordenar: risk_score DESC
Limite: Top 50
Formatação condicional por risk_score
```

### 10. Gráfico: Condições do Acidente
```
Tipo: Gráfico de Barras Agrupadas
Categorias: tipo_acidente (top 10)
Valores: COUNT(id)
Agrupado por: has_deaths
Cores: Verde (sem mortes), Vermelho (com mortes)
```

---

## 🔍 MÉTRICAS CALCULADAS

### Taxa de Mortalidade por Período
```
Fórmula: 
  Deaths = SUM(mortos)
  Accidents = COUNT(id)
  Rate = (Deaths / Accidents) * 100
Formato: % com 2 decimais
Aplicar em: time_period, day_of_week_name_pt
```

### Score Médio de Risco
```
Fórmula: AVG(composite_risk_score)
Formato: Número decimal 0-100
Contexto: Por estado, rodovia, hora
Indicador visual: 
  - 0-25: Verde
  - 26-50: Amarelo
  - 51-75: Laranja
  - 76-100: Vermelho
```

### Proporção de Acidentes Graves
```
Fórmula:
  High Risk = COUNTIF(is_high_risk = TRUE)
  Total = COUNT(id)
  Proportion = (High Risk / Total) * 100
Resultado esperado: ~20%
```

### Acidentes por Milhão de km
```
Disponível no campo: accidents_per_km (em highway_segments)
Não precisa calcular, já está pronto
```

---

## 🎨 FILTROS GLOBAIS (Aplicar em TODAS as páginas)

### 1. Filtro de Período
```
Campo: date
Tipo: Date Range Slicer
Padrão: Todos os dados
```

### 2. Filtro de Estado
```
Campo: uf
Tipo: Multi-select Dropdown
Padrão: Todos
```

### 3. Filtro de Severidade
```
Campo: has_deaths
Tipo: Toggle ou Checkbox
Opções: 
  - Todos
  - Apenas com mortes
  - Apenas com feridos
  - Sem vítimas
```

### 4. Filtro de Rodovia
```
Campo: br
Tipo: Multi-select Dropdown com busca
Top 10 mais comuns
```

### 5. Filtro de Período do Dia
```
Campo: time_period
Tipo: Radio buttons
Opções: Madrugada, Manhã, Tarde, Noite, Todos
```

---

## 📋 CAMPOS IMPORTANTES POR USO

### Para Agregações
- `id` - Contar acidentes
- `mortos` - Somar mortes
- `feridos` - Somar feridos
- `pessoas` - Somar pessoas envolvidas

### Para Análise Temporal
- `date` - Agrupamento por data
- `hour` - Análise por hora (0-23)
- `day_of_week_name_pt` - Nome do dia em português
- `month_name` - Nome do mês
- `time_period` - Período do dia (manhã/tarde/noite/madrugada)

### Para Análise Geográfica
- `uf` - Estado (sigla)
- `state_region` - Região do Brasil
- `br` - Número da rodovia
- `km` - Quilômetro
- `municipio` - Cidade
- `latitude` - Coordenada para mapa
- `longitude` - Coordenada para mapa

### Para Filtros Rápidos
- `has_deaths` - Booleano (com mortes?)
- `has_injuries` - Booleano (com feridos?)
- `is_high_risk` - Booleano (alto risco?)
- `is_weekend` - Booleano (fim de semana?)
- `is_hotspot` - Booleano (ponto crítico?)

### Para Contexto
- `accidents_this_hour` - Total de acidentes nesta hora
- `accidents_this_dow` - Total neste dia da semana
- `accidents_in_state` - Total neste estado
- `accidents_on_highway` - Total nesta rodovia

### Para Visualização
- `marker_color` - Cor já calculada para mapa
- `marker_size` - Tamanho para pontos no mapa
- `composite_risk_score` - Score principal de risco (0-100)
- `severity_score` - Score de severidade (0-100)