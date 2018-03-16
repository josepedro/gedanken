public class StreamD2Q9 {

    public static int[] getIdPosition(int id, int number_lines, int number_rows){
        int idCount = 1;
        for (int line = 0; line < number_lines; line++) {
            for (int row = 0; row < number_rows; row++) {
                if (idCount == id) {
                    int[] idPosition = {line, row};
                    return idPosition;
                } else {
                    idCount += 1;
                }
            }
        }
        return null;
    }

    public static int getPositionId(int lineId, int rowId, int number_lines, int number_rows){
        int idCount = 1;
        for (int line = 0; line < number_lines; line++) {
            for (int row = 0; row < number_rows; row++) {
                if (line == lineId && row == rowId) {
                    return idCount;
                } else {
                    idCount += 1;
                }
            }
        }
        return idCount;
    }

    public static int[] get1(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            int[] result = {number_lines - 1, row};
            return result;
        }
        if (line == 0 && row == number_rows - 1){
            int[] result = {number_lines - 1, number_rows - 1};
            return result;
        }
        if (line == 0 && row > 0 && row < number_rows - 1){
            int[] result = {number_lines - 1, row};
            return result;
        }
        else{
            int[] result = {line - 1, row};
            return result;
        }
    }

    public static int get1_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get1(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get2(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            return new int[] {number_lines - 1, row + 1};
        }
        if (line == 0 && row == number_rows - 1){
            return new int[] {number_lines - 1, 0};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {number_lines - 2, 0};
        }
        if (line == 0 && row > 0 && row < number_rows - 1){
            return new int[] {number_lines - 1, row + 1};
        }
        if (line > 0 && line < number_lines - 1 && row == number_rows - 1){
            return new int[] {line - 1, 0};
        }
        else{
            return new int[] {line - 1, row + 1};
        }
    }

    public static int get2_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get2(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get3(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == number_rows - 1){
            return new int[] {line, 0};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {number_lines - 1, 0};
        }
        if (line > 0 && line < number_lines - 1 && row == number_rows - 1){
            return new int[] {line, 0};
        }
        else{
            return new int[] {line, row + 1};
        }
    }

    public static int get3_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get3(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get4(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == number_rows - 1){
            return new int[] {1, 0};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {0, 0};
        }
        if (line == number_lines - 1 && row == 0){
            return new int[] {0, 1};
        }
        if (line > 0 && line < number_lines - 1 && row == number_rows - 1){
            return new int[] {line + 1, 0};
        }
        if (line == number_lines - 1 && row > 0 && row < number_rows - 1){
            return new int[] {0, row + 1};
        }
        else{
            return new int[] {line + 1, row + 1};
        }
    }

    public static int get4_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get4(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get5(int line, int row, int number_lines, int number_rows){
        if (line == number_lines - 1 && row == 0){
            return new int[] {0, row};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {0, row};
        }
        if (line == number_lines - 1 && row > 0 && row < number_rows - 1){
            return new int[] {0, row};
        }
        else{
            return new int[] {line + 1, row};
        }
    }

    public static int get5_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get5(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get6(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            return new int[] {line + 1, number_rows - 1};
        }
        if (line ==  number_lines - 1 && row == 0){
            return new int[] {0, number_rows - 1};
        }
        if (line == number_lines - 1 && row == number_rows - 1){
            return new int[] {0, row - 1};
        }
        if (line > 0 && line < number_lines - 1 && row == 0){
            return new int[] {line + 1, number_rows - 1};
        }
        if (line == number_lines - 1 && row > 0 && row < number_rows - 1){
            return new int[] {0, row - 1};
        }
        else{
            return new int[] {line + 1, row - 1};
        }
    }

    public static int get6_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get6(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get7(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            return new int[] {line, number_rows - 1};
        }
        if (line == number_lines - 1 && row == 0){
            return new int[] {line, number_rows - 1};
        }
        if (line > 0 && line < number_lines - 1 && row == 0){
            return new int[] {line, number_rows - 1};
        }
        else{
            return new int[] {line, row - 1};
        }
    }

    public static int get7_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get7(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }

    public static int[] get8(int line, int row, int number_lines, int number_rows){
        if (line == 0 && row == 0){
            return new int[] {number_lines - 1, number_rows - 1};
        }
        if (line == 0 && row == number_rows - 1){
            return new int[] {number_lines - 1, row - 1};
        }
        if (line == number_lines - 1 && row == 0){
            return new int[] {line - 1, number_rows - 1};
        }
        if (line > 0 && line < number_lines - 1 && row == 0){
            return new int[] {line - 1, number_rows - 1};
        }
        if (line == 0 && row > 0 && row < number_rows - 1){
            return new int[] {number_lines - 1, row - 1};
        }
        else{
            return new int[] {line - 1, row - 1};
        }
    }

    public static int get8_id(int id, int number_lines, int number_rows){
        int[] position_id = getIdPosition(id, number_lines, number_rows);
        int[] position_neighborhood = get8(position_id[0], position_id[1], number_lines, number_rows);
        return getPositionId(position_neighborhood[0], position_neighborhood[1], number_lines, number_rows);
    }
}
