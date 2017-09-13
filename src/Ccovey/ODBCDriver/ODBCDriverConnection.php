<?php namespace Ccovey\ODBCDriver;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Database\Schema\Grammars\Grammar as SchemaGrammar;

class ODBCDriverConnection extends Connection
{
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
