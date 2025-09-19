# Exportador da Biblioteca ANEEL

Este módulo converte os artefatos produzidos pelo coletor headless da
Biblioteca ANEEL para o formato consumido pelo SANDOVAL. A ferramenta
trabalha com os arquivos gerados pelo script existente, sem executar
novas raspagens.

## Fluxo de uso

1. Execute o coletor bash/python fornecido pela ANEEL no servidor de
   origem. Ele criará a estrutura a seguir:
   ```
   aneel_biblioteca/
     ├── results/
     │   ├── <TIPO>/<ANO>/<arquivo>.pdf
     │   └── _index_<ANO>.csv
     └── lists/
         └── control_master.json
   ```
2. Monte ou sincronize essa pasta no ambiente do SANDOVAL e defina a
   variável `ANEEL_BIBLIOTECA_DIR` apontando para o diretório base.
3. Configure `POSTGRES_CONNECTION_STRING` e `OPENAI_API_KEY` no mesmo
   ambiente do exportador.
4. Execute:
   ```bash
   cd legislatech-ai-crawlers
   python -m aneel_biblioteca.main --verbose
   ```
   Utilize `--dry-run` para inspecionar os documentos antes de enviar
  ao banco. Os registros são inseridos/atualizados na coleção informada
  (por padrão `michel_teste`). Ao final da execução é exibido um resumo
  destacando quantos itens foram preparados, quantos artefatos binários
  foram ignorados e quantos foram descartados por outros motivos.

## Metadados gerados

- `titulo`: âncora do índice `_index_<ano>.csv`
- `ano`: campo `year` de `control_master.json`
- `num_lei`: tentativa heurística de extrair o número do título
- `data_lei`: primeira data reconhecida no título ou texto
- `main_sancionador`: heurística (`Diretoria Colegiada`, `Diretor-Geral`
  ou `Agência Nacional de Energia Elétrica`)
- `tipo`, `link`, `pdf_url`, `arquivo_local`, `coletado_em`, `modo_origem`
  conforme os artefatos originais
- `fonte` e `colecao` são fixados como `Biblioteca ANEEL` e
  `aneel_biblioteca`

Todos os documentos recebem um identificador determinístico no formato
`aneel_biblioteca::<ano>::<tipo>::<hash>`, permitindo reprocessamento
sem duplicidade.

## Limitações conhecidas

- Somente arquivos PDF e tabelas texto (`csv`/`tsv`) são convertidos para
  texto. Demais artefatos binários (zip, planilhas Excel, docx, ppt etc.)
  ignorados explicitamente para evitar inserção incorreta e aparecem
  como `Ignorando arquivo binário` no log.
- Os campos `num_lei` e `data_lei` utilizam heurísticas simples que
  podem falhar dependendo da nomenclatura.
- Certifique-se de que os diretórios `results/` e `lists/` estejam
  acessíveis no momento da exportação.

Para integrar o fluxo completo, execute os crawlers existentes do
repositório e, em seguida, rode o exportador ANEEL antes de iniciar a
API do SANDOVAL.
