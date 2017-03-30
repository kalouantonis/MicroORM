import sqlite3


class MicroORM:

    def __init__(self, filename, table_name):
        """
        :param filename: expects a string giving the location and name of the
        database file. If it doesnt exist, it will create it

        :param table_name: the table_name that will be processed for the duration of this
        class lifecycle. If it does not exist, the user must take responsibility to
        create it
        """

        self._filename = filename
        self._table_name = table_name

        self._use_or = None
        self._sort_str = None

        self.OperationalError = sqlite3.OperationalError

        try:
            self.connection = sqlite3.connect(self._filename)
            self.cursor = self.connection.cursor()
        except sqlite3.IntegrityError as sqerr:
            print "Error: Could not make connection to database\nStack: " + str(sqerr)


    def create_table(self, **kwargs):
        """
        Creates a table with the table_name provided in the constructor

        CreateTable(self, col_name=attributes) --> Bool

        E.g. db.CreateTable(id="PRIMARY KEY AUTOINCREMENT") will result in SQL
        ---> CREATE TABLE table_name ( id PRIMARY KEY AUTOINCREMENT )

        :return: True if query succeeded or false if failed

        """
        query_string = "CREATE TABLE " + self._table_name + "(\n"

        # Iterate through each key and value in kwargs and add them on
        # to the sql string
        for index, (key, value) in enumerate(zip(kwargs.keys(), kwargs.values())):
            query_string += key + ' ' + value + self._append_comma(index, len(kwargs))

            # Check if the item is the last one, if it is not append
            # a ','

        query_string += ')'

        print query_string

        return self._exec_query(query_string)


    def insert(self, **kwargs):
        """
        Inserts in to the table the given values in to the given columns

        Insert(self, col_name=insert_val) --> Bool

        E.g. db.Insert(name="John Smith") will result to
        ---> INSERT INTO table_name (`name`) VALUES ("John Smith")

        :return: True if query succeeded or false if failed

        """

        query_string = "INSERT INTO " + self._table_name
        query_keys = "("
        query_vals = "("

        for index, (key, value) in enumerate(zip(kwargs.keys(), kwargs.values())):
            query_keys += '`' + key + '`'

            query_vals += self._check_type(value)

            query_keys += self._append_comma(index, len(kwargs))
            query_vals += self._append_comma(index, len(kwargs))


        query_string += query_keys + ") VALUES " + query_vals + ")"

        print query_string

        return self._exec_query(query_string)


    def get(self):
        """
        Fetches all data (rows, columns) from the table

        db.Get(self) --> Bool

        :return: List of all items in DB or None if query failed

        :example: db.Get() corresponds to this SQL
        ---> SELECT * FROM table_name

        """

        return self.select()


    def get_where(self, **where):
        """
        Selects all columns from given positions in the table

        db.GetWhere(self, col1=val1, col2=val2) --> Bool

        :return: List of items in query or None if query failed

        :example: db.GetWhere(id=5, task="Get Milk") corresponds to this SQL
        ---> SELECT * FROM table_name WHERE `id` = 5 AND `task` = "Get Milk"

        """

        return self.select(**where)

    def get_like(self, *cols ,**like):
        """
        Gets items from the Database that have a similarity to the given like kwargs

        :example: db.GetLike('id', task="Get") Corresponds to this SQL
        ---> SELECT `id` FROM table_name WHERE `task` LIKE "%Get%"

        :param cols: The columns that are required
        :param like: Same as **where in Select, except the LIKE %value% sql query is made
        :return: None or List
        """
        query_string = self._gen_select(*cols)
        if len(like) > 0:
            query_string += " WHERE "

            for index, (key, value) in enumerate(zip(like.keys(), like.values())):
                query_string += '`' + key + '`' + " LIKE "

                if type(value) is str:
                    query_string += "\"%" + value + "%\""
                else:
                    query_string += str(value)

                query_string += self._check_op(index, len(like))


        print query_string

        return self._exec_select(query_string)


    def use_or(self, flag):
        self._use_or = flag

    def sort(self, col_name, asc=True):

        self._sort_str = ' ORDER BY ' + str(col_name)

        if asc:
            self._sort_str += ' ASC'
        else:
            self._sort_str += ' DESC'



    def select(self, *cols, **where):
        """
        Selects specified columns, if not specified, selects all. Uses the
        where arguments to select specific items

        Select(self, col1, col2, id=id_num, name="Some name") --> list or False

        E.g. Select('task', id=3) results to
        ---> SELECT `task` FROM table_name WHERE `id`=3

        :return: None if query failed or a list of items if query succeeded

        """

        query_string = self._gen_select(*cols)

        if len(where) > 0:
            query_string += " WHERE "

            for index, (key, value) in enumerate(zip(where.keys(), where.values())):
                query_string += '`' + key + '`' + " = " + self._check_type(value) \
                                + self._check_op(index, len(where))


        print query_string

        return self._exec_select(query_string)

    def update(self, set_dict, **where):

        """
        Calls the SQL UPDATE function according to supplied set arguments and where arguments

        :param set_dict: dictionary containing all the details for the SQL SET function.
        SET KEY1=VAL1
        :param where:  values corresponding to SQL WHERE --> WHERE KEY1=VAL1
        :return: returns true if query successful or false if failed
        """
        query_string = "UPDATE " + self._table_name + " SET "

        for index, (key, value) in enumerate(zip(set_dict.keys(), set_dict.values())):
            query_string += '`' + key + '`' + " = " + self._check_type(value) \
                            + self._append_comma(index, len(set_dict))


        query_string += " WHERE "

        for index, (key, value) in enumerate(zip(where.keys(), where.values())):
            query_string += '`' + key + '`' + " = " + self._check_type(value) \
                            + self._check_op(index, len(where))


        print query_string

        return self._exec_query(query_string)


    def delete(self, **where):
        """
        Deletes an item from the table depending on the supplied where arguments.

        :example: Delete(id=4) --> DELETE FROM table_name WHERE `id` = 4

        :param where: corresponds to values in the WHERE function. For example,
        key1=val1
        :return: returns True if query succeeded or false if failed
        """
        query_string = "DELETE FROM " + self._table_name + " WHERE "

        for index, (key, value) in enumerate(zip(where.keys(), where.values())):
            query_string += '`' + key + '`' + " = " + self._check_type(value) \
                            + self._check_op(index, len(where))


        print query_string

        return self._exec_query(query_string)

    # PRIVATE METHODS ###################################
    # Do not use these out

    def _gen_select(self, *cols):
        """
        Generates the initial part of the select statement, so that the self.Select
        and the self. GetLike methods do not repeat themselves

        :example: self._gen_select('id', 'task') Corresponds to this SQL
        --> SELECT `id`, `task` FROM table_name

        :param cols: The columns that want to be queried
        :return: String containing the generated select statement
        """
        query_string = "SELECT "

        # Check if columns are specified
        if len(cols) < 1:
            query_string += "*"
        else:
            for index, each_col in enumerate(cols):
                query_string += '`' + each_col + '`' + self._append_comma(index, len(cols))

        query_string += " FROM " + self._table_name

        return query_string

    def _exec_select(self, query_string):
        """
        Execute a select statement from a previously generated statement.
        Will return a list containing each row of data, with a nested dictionary
        with keys as the column names and the values as the row values in those
        columns.

        :param query_string: The query statement that is to be executed
        :return: List or None
        """

        try:
            query_string += str(self._sort_str)

            results = self.cursor.execute(query_string)

            col_names = [i[0] for i in self.cursor.description]

            response = [row for row in results.fetchall()]

            # Select items and sort them in to keys and values according to their column
            # names and the given values from the SQL query
            # The columns are returned from the cursor.description, getting the column names
            # only specified in the query, so there shouldn't be any issues
            return_val = []
            for each_row in response:
                temp = {}
                for col_index, each_col in enumerate(col_names):
                    temp[each_col] = each_row[col_index]
                return_val.append(temp)

            return return_val

        except sqlite3.IntegrityError as sqerr:
            print "Error in Select statement: " + str(sqerr)

            return None


    @staticmethod
    def _append_comma(index, size):
        """
        Checks if the current index is at the location of the last item in the
        dictionary supplied. If it's not, it appends a comma,
        so to correspond to sql syntax

        _append_comma(self, index, dictionary) --> String

        :param index: an integer value of the position the iterator is through the dictiorary
        :param size: the size of the dictionary or kwargs or list
        :return: a string of either ', ' or an empty string

        """

        if index != (size - 1):
            return ', '

        return ''


    def _check_op(self, index, size):
        if self._use_or:
            return self._append_or(index, size)

        return self._append_and(index, size)

    @staticmethod
    def _append_and(index, size):
        """
        Checks if the current index is at the location of the last item in the
        dictionary supplied. If it's not, it appends an AND, as so to correspond
        to SQL syntax


        :param index: an integer value of the position the iterator is through the dictionary
        :param size: the size of the dictionary or kwargs or list
        :return: string, either an ' AND ' or an empty string
        """

        if index != (size - 1):
            return ' AND '

        return ''

    @staticmethod
    def _append_or(index, size):
        if index != (size - 1):
            return ' OR '

        return ''

    @staticmethod
    def _check_type(value):
        if (type(value) is str) or (type(value) is unicode) or (type(value) is chr):
            return "\"" + value + "\""

        return str(value)


    def _exec_query(self, query_string):
        try:
            data = self.cursor.execute(query_string)
            self.connection.commit()

            return data.lastrowid
        except sqlite3.IntegrityError as sqerr:
            print "Error in query! Query String --> " + query_string \
                    + "SQL Error: " + str(sqerr)
            return False

    # Operators ########################
    def __len__(self):
        """
        Returns the number of rows in the SQL table

        :return: Integer containing the number of Rows in the table
        """

        return len(self.get())


    def __getitem__(self, item):
        """
        Extracts all data from a column in the database

        :param item: String containing the column name that needs extracting
        :return: List or None
        """
        return self.select(item)


    def __delitem__(self, key):
        """
        Remove an item from the database, with that given ID

        :param key: The id of the item to be deleted
        :return: True or False depending if query succeeded
        """

        self.delete(id=key)

    # Destructor #######################
    def __del__(self):
        """
        Destructor, destroy the connection and cursor
        """

        self.connection.close()


# Testing
if __name__ == '__main__':
    db = MicroORM('test.sqlite', 'table2')
    #db.CreateTable(id="INTEGER PRIMARY KEY AUTOINCREMENT", task="VARCHAR(100)", desc="TEXT")
    #db.Insert(task="Get Milk", desc="Get some more milk")
    #print db.Insert(task="Get Food", desc="Im soo hungry!")

	#db.Get()

	




