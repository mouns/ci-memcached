<?php if ( ! defined('BASEPATH')) exit('No direct script access allowed');

/**
 *
 * Library for handling libmemcached pools
 *
 * @link http://php.net/manual/en/book.memcached.php
 * @link http://en.wikipedia.org/wiki/Memcached
 * @link http://en.wikipedia.org/wiki/Memcachedb
 *
 * @author Nathanaelle <https://github.com/nathanaelle>
 *
 * @license combinaison of (BSD (cond 1,2,5) + Art 13 AGPL (cond 3,4) )
 *
 * Copyright (c) 2010-2011, MOUNS <https://github.com/mouns>
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     1 Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     2 Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     3 You have permission to link or combine any work with this work into a
 *       single combined work, and to convey the resulting work.
 *       The terms of this license will continue to apply to this work, but the
 *       work with which it is combined will remain governed by its own license.
 *     4 If you modify the source or the binary, your modified version must
 *       prominently offer all users interacting with it remotely through a
 *       computer network (if your version supports such interaction) an
 *       opportunity to receive your version by providing access to the
 *       corresponding distribution from a network server at no charge, through
 *       some standard or customary means of facilitating copying of software.
 *     5 Neither the name of MOUNS nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY MOUNS AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL MOUNS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
class CI_Memcached	{

	private $config;
	private $m;
	private $ci;

	/**
	 *
	 * Configure the object and establish the connexion to the libmemcached pool
	 */
	public function __construct()	{
		$this->ci =& get_instance();
		$this->m = FALSE;

		if(class_exists('Memcached'))	{
			$this->ci->load->config('memcached');
			$this->config = $this->ci->config->item('memcached');

			if(!extension_loaded('Memcached'))
			trigger_error('Memcached Extension must be loaded',E_CORE_ERROR);

			$this->m = new Memcached();
			log_message('debug', "Memcached Library: Memcached Class Loaded");

			/**
			 * Setting options for the optimal use of the pool
			 *
			 * @link http://en.wikipedia.org/wiki/Consistent_hashing
			 *
			 */
			$this->m->setOption( Memcached::OPT_HASH, Memcached::HASH_MD5 );
			$this->m->setOption( Memcached::OPT_DISTRIBUTION, Memcached::DISTRIBUTION_CONSISTENT );
			$this->m->setOption( Memcached::OPT_LIBKETAMA_COMPATIBLE, true );
			$this->m->setOption( Memcached::OPT_NO_BLOCK, true );
			$this->auto_connect();
		}
	}



	/**
	 *
	 * runs through all of the servers defined in
	 * the configuration and attempts to connect to each
	 *
	 * @return none
	 */
	protected function auto_connect()	{
		foreach($this->config['servers'] as $key=>$server)	{
			if(!$this->add_server($server)){
				log_message('error', 'Memcached Library: Could not connect to the server named "'.$key.'"');
			} else
			log_message('debug', 'Memcached Library: Successfully connected to the server named "'.$key.'"');
		}
	}


	/**
	 *
	 * Add a server in the pool
	 *
	 * @param string	$server
	 *
	 * @return cf. Memcached::addServer()
	 */
	protected function add_server($server)	{
		extract($server);
		return $this->m->addServer($host, $port, $persistent );
	}


	/**
	 * add a new key if the key doesn't exist.
	 * do nothing if the key already exists.
	 *
	 * @param	mixed	$key
	 * @param	mixed	$value
	 * @param	integer	$expiration
	 */
	public function add($key, $value, $expiration = NULL) {
		if(!is_int($expiration))
		$expiration = $this->config['expiration'];

		if(!is_array($key)) {
			$key	= array( $key );
			$value	= array( $value );
		}
		$kvz = array_combine( $key, $value );

		foreach($kvz as $key => $value )
		$this->m->add( $key, $value, $expiration );
	}


	/**
	 * set a key even if the key already exists.
	 *
	 * @param	mixed	$key
	 * @param	mixed	$value
	 * @param	integer	$expiration
	 */
	public function set($key, $value, $expiration = NULL) {
		if(!is_int($expiration))
		$expiration = $this->config['expiration'];

		if(is_array($key)) {
			$kvz = array_combine( $key, $value );
			$thid->m->setMulti( $kvz, $expiration );
		} else
		$this->m->set( $key, $value, expiration );
	}


	/**
	 * increment a value's key if the key exists.
	 * do nothing if the key doesn't exist.
	 *
	 * @param	string	$key
	 * @param	mixed	$value
	 *
	 * @return bool
	 */
	public function increment($key, $value=1) {
		return $this->m->increment( $key, $value );
	}


	/**
	 * decrement a value's key if the key exists.
	 * do nothing if the key doesn't exist.
	 *
	 * @param	string	$key
	 * @param	mixed	$value
	 *
	 * @return bool
	 */
	public function decrement($key, $value=1) {
		return $this->m->decrement( $key, $value );
	}


	/**
	 * prepend a value's key if the key exists.
	 * do nothing if the key doesn't exist.
	 *
	 * @param	string	$key
	 * @param	mixed	$value
	 *
	 * @return bool
	 */
	public function prepend($key, $value) {
		return $this->m->prepend( $key, $value );
	}


	/**
	 * append a value's key if the key exists.
	 * do nothing if the key doesn't exist.
	 *
	 * @param	string	$key
	 * @param	mixed	$value
	 *
	 * @return bool
	 */
	public function append($key, $value) {
		return $this->m->append( $key, $value );
	}


	/**
	 * append a value's key if the key exists.
	 * create the key if the key doesn't exist.
	 *
	 * the algorithm a case of race condition :
	 *  if append() fails	=> key doesn't exist
	 *  if add() fails		=> key does exist now because another add happens
	 *  key does exists but a pool defect is possible so recurse
	 *
	 * @todo find a "clever" way to prevent the infinite loop :p
	 *
	 * @param	string	$key
	 * @param	mixed	$value
	 * @param	integer	$expiration
	 */
	protected function append_or_add($key, $value, $expiration = NULL) {
		if(!is_int($expiration))
			$expiration = $this->config['expiration'];
		return	$this->m->append( $key, $value )
		or		$this->m->add( $key, $value, $expiration )
		or		$this->m->append_or_add( $key, $value, $expiration );
	}


	/**
	 * add a set of elements in a key
	 *
	 * @param	string	$key
	 * @param	array	$set
	 */
	public function set_insert( $key, $set ) {
		if(!is_array($set)) $set = array( $set );
		$encoded='';
		foreach( $set as $k )
		$encoded .= '+'.base64_encode($k).' ';
		$this->append_or_add( $key, $encoded );
	}


	/**
	 * remove a set of elements from a key
	 *
	 * @param	string	$key
	 * @param	array	$set
	 */
	public function set_remove( $key, $set ) {
		if(!is_array($set)) $set = array( $set );
		$encoded='';
		foreach( $set as $k )
		$encoded .= '-'.base64_encode($k).' ';
		$this->append_or_add( $key, $encoded );
	}


	/**
	 * list the set of elements of a key
	 *
	 * @param	string	$key
	 * @param	array	$set
	 *
	 * @return array
	 */
	public function set_items( $key ) {
		$val = preg_split('/ /', $this->m->get( $key, null, $cas ), -1, PREG_SPLIT_NO_EMPTY);

		$set = array();
		$operation=0;
		foreach( $val as $v )
		switch( $val[0] ) {
			case '+':
				$set[base64_decode(substr($val,1))]=1;
				$operation++;
				break;
			case '-':
				unset($set[base64_decode(substr($val,1))]);
				$operation++;
				break;
			default:
		}
		$set = array_keys( $set );

		if( count( $set )/$operation < 0.95 ) {
			$encoded='';
			foreach( $set as $k )
			$encoded .= '+'.base64_encode($k).' ';
			$this->m->cas( $cas, $key, $encoded, $this->config['expiration'] );
		}

		return $set;
	}


	/**
	 *
	 * Gets the data for a single key or an array of keys
	 *
	 * @param	mixed	$key
	 *
	 * @return
	 */
	public function get($key) {
		if(is_array($key))
		return $this->m->getMulti( $key );
		return $this->m->get( $key );
	}


	/**
	 *
	 * delete a key.
	 *
	 * @param	mixed	$key
	 * @param	integer	$expiration		delay before the deletion
	 */
	public function delete($key)	{
		if(!array($key))
		$key = array( $key );

		foreach($key as $k)
		$this->m->delete( $k );
	}


	/**
	 *
	 * replace a key that already exists.
	 * do nothing if the key doesn't exist.
	 *
	 * @param	mixed	$key
	 * @param	mixed	$value
	 * @param	integer	$expiration
	 */
	public function replace($key, $value, $expiration = NULL) {
		if(!is_int($expiration))
		$expiration = $this->config['expiration'];

		if(!is_array($key)) {
			$key	= array( $key );
			$value	= array( $value );
		}
		$kvz = array_combine( $key, $value );

		foreach($kvz as $key => $value )
		$this->m->replace( $key, $value, expiration );
	}



}
