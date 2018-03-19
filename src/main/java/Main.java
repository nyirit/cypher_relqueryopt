import org.neo4j.driver.v1.*;

public class Main implements AutoCloseable
{
    private final String DELETE_EVERYTHING =
            "MATCH(n)" +
            "DETACH DELETE n " +
            "RETURN COUNT(n)";

    private final String CREATE_DATA =
            "create " +
                "(prod:Production:Operator), " +
                "(proj:Projection:Operator {attributes: ['a', 'b']}), " +
                "(proj2:Projection:Operator {attributes: ['a', 'b']}), " +
                "(sel:Selection:Operator {attribute: 'a', arithm: '=', const:'2'}), " +
                "(join:Join:Operator), " +
                "(t1:Relation:Operator {schema: ['a', 'b', 'c']}), " +
                "(t2:Relation:Operator {schema: ['c', 'd']}), " +
                "(prod)-[:CHILD]->(proj2)-[:CHILD]->(proj)-[:CHILD]->(sel)-[:CHILD]->(join)-[:CHILD {side:'left'}]->(t1), " +
                "(join)-[:CHILD {side: 'right'}]->(t2)";

    private final String SCHEMA_DEDUCTION_JOIN =
            "MATCH (r1:Operator)<-[:CHILD {side: 'left'}]-(j1:Join)-[:CHILD {side: 'right'}]->(r2:Operator)\n" +
            "WHERE\n" +
                "j1.schema IS NULL AND\n" +
                "r1.schema IS NOT NULL AND\n" +
                "r2.schema IS NOT NULL\n" +
            "SET j1.schema = r1.schema + [a IN r2.schema WHERE NOT a IN r1.schema]\n" +
            "RETURN count(*) as count";

    private final String SCHEMA_DEDUCTION_SELECT =
            "MATCH (child:Operator)<-[edge:CHILD]-(parent:Operator)\n" +
            "WHERE\n" +
                "edge.side IS NULL AND\n" +
                "child.schema IS NOT NULL AND\n" +
                "parent.schema IS NULL AND\n" +
                "NOT parent:Projection\n" +
            "SET parent.schema = child.schema\n" +
            "RETURN count(*) as count";

    private final String SCHEMA_DEDUCTION_PROJECTION =
            "MATCH (proj:Projection)\n" +
            "SET proj.schema = proj.attributes\n" +
            "RETURN COUNT(proj)";

    private final String DELETE_USELESS_PROJ =
            "MATCH (parent:Operator)-[pe:CHILD]->(proj:Projection)-[ce:CHILD]->(child:Operator)\n" +
            "WHERE child.schema = proj.schema\n" +
            "CREATE (parent)-[:CHILD]->(child)\n" +
            "DETACH DELETE proj\n" +
            "RETURN count(proj)";

    private final String PROJ_PUSHDOWN = "" +
            "MATCH (parent:Operator)-[e1:CHILD]->(proj:Projection)-[e2:CHILD]->(sel:Selection)-[e3:CHILD]->(child:Operator)\n" +
            "WHERE sel.attribute IN proj.attributes\n" +
            "CREATE (parent)-[:CHILD]->(sel)-[:CHILD]->(proj)-[:CHILD]->(child)\n" +
            "SET sel.schema = proj.schema\n" +
            "WITH e1, e2, e3, COUNT(proj) as count\n" +
            "DELETE e1, e2, e3\n" +
            "RETURN count";

    private final String DELETE_USELESS_JOIN = "" +
            "MATCH (proj:Projection)-[e1:CHILD]->(join:Join)-[er:CHILD {side: 'right'}]->(rop:Operator),\n" +
            "(join)-[el:CHILD {side: 'left'}]->(lop:Operator)\n" +
            "WHERE [a IN proj.schema WHERE NOT a IN lop.schema] = []\n" +
                "CREATE (proj)-[:CHILD]->(lop)\n" +
                "WITH e1, er, el, join, COUNT(proj) as count\n" +
                "DETACH DELETE join\n" +
                "DELETE e1, er, el\n" +
                "RETURN count";

    private final Driver driver;

    private Main( String uri, String user, String password )
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    private void reinit()
    {
        try (Session session = driver.session())
        {
            session.writeTransaction( new TransactionWork<String>()
            {
                @Override
                public String execute(Transaction tx)
                {
                    Integer remove_result = tx.run(DELETE_EVERYTHING).single().get(0).asInt();
                    System.out.println(remove_result + " nodes removed.");

                    tx.run(CREATE_DATA);

                    return "ok";
                }
            } );
        }
    }

    private void deduceSchema() {
        try (Session session = driver.session())
        {
            session.writeTransaction( new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    // Deduce projection
                    tx.run(SCHEMA_DEDUCTION_PROJECTION).single().get(0).asInt();

                    // Deduce join and select
                    Integer join, sel;
                    do {
                        join = tx.run(SCHEMA_DEDUCTION_JOIN).single().get(0).asInt();
                        sel = tx.run(SCHEMA_DEDUCTION_SELECT).single().get(0).asInt();
                    } while (join + sel > 0);

                    return "ok";
                }
            });
        }
    }

    private void optimize()
    {
        try (Session session = driver.session()) {
            session.writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    Integer deepen_proj;
                    do {
                        StatementResult sr = tx.run(PROJ_PUSHDOWN);
                        deepen_proj = sr.hasNext() ? sr.single().get(0).asInt() : 0;
                        System.out.println("Deepen projection count: " + deepen_proj);
                    } while(deepen_proj > 0);

                    Integer removed_proj = tx.run(DELETE_USELESS_PROJ).single().get(0).asInt();
                    System.out.println("Removed projection count: " + removed_proj);

                    Integer deleted_join = tx.run(DELETE_USELESS_JOIN).single().get(0).asInt();
                    System.out.println("Deleted join count: " + deleted_join);

                    return "ok";
                }
            });
        }
    }

    public static void main( String... args ) throws Exception
    {
        try (Main main = new Main( "bolt://localhost:7687", "neo4j", "admin" ))
        {
            main.reinit();
            main.deduceSchema();
            main.optimize();
        }
    }
}