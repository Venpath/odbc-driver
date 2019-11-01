<?php namespace Ccovey\ODBCDriver;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Database\Schema\Grammars\Grammar as SchemaGrammar;

class ODBCDriverConnection extends Connection
{
	/**
 	* Run a select statement against snowflake.  It can't do PDO for now.
 	*
 	* @param  string  $query
 	* @return array
	*/
	public function snowflake($query)
	{
		return $this->run($query, [], function ($me, $query) {
			if (!is_string($me)) {
				if ($me->pretending()) {
					return [];
				}
			} else {
				if ($this->pretending()) {
					return [];
				}

				$query = $me;
			}

			// Set up a manual ODBC connection to Snowflake
			$conn = odbc_connect($this->config['dsn'], $this->config['username'], $this->config['password']);

			if (!$conn) {
				throw new \Exception("Connection Failed: " . $conn);
			}

			$rs = odbc_exec($conn, $query);

			$results = array();
			
			while($result = odbc_fetch_array($rs)) {
				// Convert to an of array objects, since that's what happens natively in the framework.
				$object = new \stdClass;

				foreach ($result as $key => $value) {
					$object->$key = $value;
				}

				$results[] = $object;
			}
			
			odbc_close($conn);
			
			unset($object);

			return $results;
		});
	}

	/**
	 * @return Query\Grammars\Grammar
	 */
	protected function getDefaultQueryGrammar()
	{
		$grammarConfig = $this->getGrammarConfig();

		if ($grammarConfig) {
			$packageGrammar = "Ccovey\\ODBCDriver\\Grammars\\" . $grammarConfig; 
			if (class_exists($packageGrammar)) {
				return $this->withTablePrefix(new $packageGrammar);
			}
			
			$illuminateGrammar = "Illuminate\\Database\\Query\\Grammars\\" . $grammarConfig;
			if (class_exists($illuminateGrammar)) {
				return $this->withTablePrefix(new $illuminateGrammar);
			}
		}

		return $this->withTablePrefix(new Grammar);
	}

	/**
	 * Default grammar for specified Schema
	 * @return Schema\Grammars\Grammar
	 */
	protected function getDefaultSchemaGrammar()
	{
		return $this->withTablePrefix(new Schema\Grammars\Grammar);
	}

	protected function getGrammarConfig()
	{
		if ($this->getConfig('grammar')) {
			return $this->getConfig('grammar');
		}

		return false;
	}

	function select($query, $bindings = Array()) {
		try {
			$return = parent::select($query, $bindings);
		 } catch (\Exception $e) {
			if (stristr($e->getMessage(), 'SQLSTATE[SL009]') !== false) {
			  	// This is good, no worries.  It ran!           
			} else {
				throw $e;
			}
		}
		
		return $return;
	}
}
