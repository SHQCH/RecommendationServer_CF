
public class MovieRecommanderApp {

    public static void main(String[] args) throws Exception{

        /**
            args0: original dataset
            args1: output dir for ItemIndexData job
            args2: output dir for UserIndexedData job
            args3: output dir for PreSimilarity job
            args4: output dir for Multiplication job
            args5: output dir for Sum job

         **/
        ItemIndexedData itemIndexedData = new ItemIndexedData();
        UserIndexedData userIndexedData = new UserIndexedData();
        PreSimilarity simMatrix = new PreSimilarity();
        Multiplication multiplication = new Multiplication();
        Sum sum = new Sum();

        String rawInput = args[0];
        String itemIndexedDataDir = args[1];
        String userIndexedDataDir = args[2];
        String simMatrixDir = args[3];
        String multiplicationDir = args[4];
        String sumDir = args[5];

        String[] path1 = {rawInput, itemIndexedDataDir};
        String[] path2 = {itemIndexedDataDir, userIndexedDataDir};
        String[] path3 = {userIndexedDataDir, simMatrixDir};
        String[] path4 = {simMatrixDir, itemIndexedDataDir, multiplicationDir};
        String[] path5 = {multiplicationDir, sumDir};


        itemIndexedData.main(path1);
        userIndexedData.main(path2);
        simMatrix.main(path3);
        multiplication.main(path4);
        sum.main(path5);

    }
}
