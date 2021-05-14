import query.insert.Insert;

public class TestJava {
    public static void main(String[] args) {
        String insert = new Insert().into(User.Companion).sql();

        System.out.println(insert);
    }
}
