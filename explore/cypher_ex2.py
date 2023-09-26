from neo4j import GraphDatabase

uri = "bolt://localhost:7687"
username = "neo4j"
password = "1qazXSW@"
database = 'biocypher'

driver = GraphDatabase.driver(uri, auth=(username, password), database=database)

with driver.session() as session:
    result = session.run("""
        CALL apoc.import.csv({
            fileName: '/home/sha/work/biocypher_tutor/biocypher-out/20230729151108/Protein-header.csv',
            arrayDelimiter: '|',
            quoteChar: "'",
            delimiter: ',',
            header: true,
            skip: 0,
            mapping: {
                Protein_ID: {type: 'string'},
                Protein_Name: {type: 'string'},
                Gene_Name: {type: 'string'},
                Length: {type: 'int'},
                Sequence: {type: 'string'}
            }
        }, 'MATCH (n) RETURN n', {});
    """)
    print(result.summary().counters)

    result = session.run("""
        CALL apoc.import.csv({
            fileName: '/home/sha/work/biocypher_tutor/biocypher-out/20230729151108/Protein-part.*',
            arrayDelimiter: '|',
            quoteChar: "'",
            delimiter: ',',
            header: true,
            skip: 0,
            mapping: {
                Protein_ID: {type: 'string'},
                Protein_Name: {type: 'string'},
                Gene_Name: {type: 'string'},
                Length: {type: 'int'},
                Sequence: {type: 'string'}
            }
        }, 'MATCH (n) RETURN n', {});
    """)
    print(result.summary().counters)