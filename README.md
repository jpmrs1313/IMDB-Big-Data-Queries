# Data_Processing_HDFS_Map_Reduce

As tarefas de processamento de dados a realizar são as seguintes:


1. Carregue os dados dos ficheiros title.basics.tsv.gz e title.ratings.tsv.gz para um único
ficheiro AvroParquet com um esquema apropriado.

2. Usando o ficheiro resultante da alinea anterior e considerando apenas filmes (movie), calcule para cada ano:
• o número total de filmes;
• o filme que recolheu mais votos;
• os 10 melhores filmes segundo a classificação.
Estes resultados devem ser armazenados num ficheiro AvroParquet com um esquema apropriado.

3. Para cada filme, recomende o outro do mesmo género que tenha a melhor classficação. Considere
apenas o primeiro género de cada filme. Estes resultados devem ser apresentados em ficheiros de texto e deve
evitar carregar em memória simultâneamente todos os filmes do mesmo género. 1
