class DBHandler(object):
    """mysql client, turn exception into check status
    """
    def __init__(self, conf):
        self.conf = conf
        self.hanler = None

    def on_init(self):
        """init, check conf , connect to server
        """
        if (self.conf is None or
            self.conf.get("host", None) is None or
            self.conf.get("port", None) is None or
            self.conf.get("user", None) is None or
            self.conf.get("passwd", None) is None or
            self.conf.get("db", None) is None):
            log.warn("db conf check failed, there is None area")
            return False

        try:
            dbcharset = "utf8"
            if "charset" in self.conf.keys():
                dbcharset = self.conf["charset"]
            self.hanler = MySQLdb.connect(
                        host=self.conf["host"],
                        port=int(self.conf["port"]),
                        user=self.conf["user"],
                        passwd=self.conf["passwd"],
                        db=self.conf["db"],
			charset=dbcharset)
            self.hanler.autocommit(True)
        except MySQLdb.OperationalError as ex:
            log.warn("failed to connect to [host: %s] [port: %d]"
                  "[user: %s] [passwd: %s] [db: %s] [exception %s %s] "
                  % (self.conf["host"], int(self.conf["port"]), self.conf["user"],
                    self.conf["passwd"], self.conf["db"], ex.args, ex.message))
            return False

        except TypeError as ex:
            log.warn("failed to connect to [host: %s] [port: %d]"
                  "[user: %s] [passwd: %s] [db: %s] [exception %s %s] "
                  % (self.conf["host"], self.conf["port"], self.conf["user"],
                    self.conf["passwd"], self.conf["db"], ex.args, ex.message))
            return False

        return True

    def query(self, sql, args=None):
        """do query, add check
        """
        #TODO special cursor for batch of data
        if self.hanler is None:
            return False, None
        cur = self.hanler.cursor()
        try:
            cur.execute(sql, args)
        except MySQLdb.ProgrammingError as ex:
            log.warn("failed to execute [sql: %s] [exception %s %s] "
                   % (sql, ex.args, ex.message))
            return False, None

        result = cur.fetchall()
        return True, result

    def insert(self, sql, args):
        """do insert, add check
        """
        if self.hanler is None:
            return False
        cur = self.hanler.cursor()
        ret = 0
        try:
            ret = cur.executemany(sql, args)
        except MySQLdb.ProgrammingError as ex:
            log.warn("failed to execute [sql: %s] [exception %s %s] "
                   % (sql, ex.args, ex.message))
            return False
        except MySQLdb.OperationalError as ex:
            log.warn("failed to execute [sql: %s] [exception %s %s] "
                   % (sql, ex.args, ex.message))
            return False
        except Exception as ex:
            log.warn("failed to execute [sql: %s] [exception %s] " % (sql, str(ex)))
            return False
        return True

    def update(self, sql, args=None):
        """do update
        """
        if self.hanler is None:
            return False
        cur = self.hanler.cursor()
        ret = 0
        try:
            ret = cur.execute(sql, args)
        except MySQLdb.ProgrammingError as ex:
            log.warn("failed to execute [sql: %s] [exception %s %s] "
                    % (sql, ex.args, ex.message))
            return False
        except MySQLdb.OperationalError as ex:
            log.warn("failed to execute [sql: %s] [exception %s %s] "
                     % (sql, ex.args, ex.message))
            return False
        except Exception as ex:
            log.warn("failed to execute [sql: %s] [exception %s] " % (sql, str(ex)))
            return False
        return True
